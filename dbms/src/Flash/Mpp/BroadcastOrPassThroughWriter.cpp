// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
template <class StreamWriterPtr>
BroadcastOrPassThroughWriter<StreamWriterPtr>::BroadcastOrPassThroughWriter(
    StreamWriterPtr writer_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
{
    rows_in_blocks = 0;
    RUNTIME_CHECK(encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        encodeThenWriteBlocks<true>();
    }
    else
    {
        encodeThenWriteBlocks<false>();
    }
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        encodeThenWriteBlocks<false>();
}

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::encodeThenWriteBlocks()
{
    TrackedMppDataPacket tracked_packet(current_memory_tracker);
    if constexpr (send_exec_summary_at_last)
    {
        TrackedSelectResp response;
        addExecuteSummaries(response.getResponse(), /*delta_mode=*/false);
        tracked_packet.serializeByResponse(response.getResponse());
    }
    if (blocks.empty())
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(tracked_packet.getPacket());
        }
        return;
    }
    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        chunk_codec_stream->encode(block, 0, block.rows());
        blocks.pop_back();
        tracked_packet.addChunk(chunk_codec_stream->getString());
        chunk_codec_stream->clear();
    }
    assert(blocks.empty());
    rows_in_blocks = 0;
    writer->write(tracked_packet.getPacket());
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::asyncWrite(Block && block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.emplace_back(std::move(block));
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        asyncEncodeThenWriteBlocks();
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::asyncFinishWrite()
{
    asyncEncodeThenWriteBlocks();
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::asyncEncodeThenWriteBlocks()
{
    if (blocks.empty())
        return;
    TrackedMppDataPacket tracked_packet(current_memory_tracker);
    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        chunk_codec_stream->encode(block, 0, block.rows());
        blocks.pop_back();
        tracked_packet.addChunk(chunk_codec_stream->getString());
        chunk_codec_stream->clear();
    }
    assert(blocks.empty());
    rows_in_blocks = 0;

    assert(!not_ready_packet.has_value() && not_ready_partitions.empty());
    for (uint16_t part_id = 0; part_id < writer->getPartitionNum(); ++part_id)
    {
        if (!writer->asyncWrite(tracked_packet.getPacket(), part_id))
            not_ready_partitions.emplace_back(part_id);
    }
    if (!not_ready_partitions.empty())
        not_ready_packet.emplace(std::move(tracked_packet));
}

template <class StreamWriterPtr>
bool BroadcastOrPassThroughWriter<StreamWriterPtr>::asyncIsReady()
{
    if (!not_ready_packet.has_value())
        return true;

    const auto & packet = not_ready_packet.value().getPacket();
    assert(!not_ready_partitions.empty());
    auto iter = not_ready_partitions.begin();
    while (iter != not_ready_partitions.end())
    {
        if (writer->asyncWrite(packet, *iter))
            iter = not_ready_partitions.erase(iter);
        else
            ++iter;
    }
    if (not_ready_partitions.empty())
    {
        not_ready_packet.reset();
        return true;
    }
    else
    {
        return false;
    }
}

template class BroadcastOrPassThroughWriter<MPPTunnelSetPtr>;

} // namespace DB
