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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <class StreamWriterPtr>
StreamingDAGResponseWriter<StreamWriterPtr>::StreamingDAGResponseWriter(
    StreamWriterPtr writer_,
    Int64 records_per_chunk_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
    , rows_in_blocks(0)
{
    switch (encode_type)
    {
    case tipb::EncodeType::TypeDefault:
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
        break;
    case tipb::EncodeType::TypeChunk:
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
        break;
    case tipb::EncodeType::TypeCHBlock:
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
        break;
    default:
        throw TiFlashException("Unsupported EncodeType", Errors::Coprocessor::Internal);
    }
    /// For other encode types, we will use records_per_chunk to control the batch size sent.
    batch_send_min_limit = encode_type == tipb::EncodeType::TypeCHBlock
        ? batch_send_min_limit
        : (records_per_chunk - 1);
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::finishWrite()
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
void StreamingDAGResponseWriter<StreamWriterPtr>::write(const Block & block)
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
void StreamingDAGResponseWriter<StreamWriterPtr>::encodeThenWriteBlocks()
{
    TrackedSelectResp response;
    if constexpr (send_exec_summary_at_last)
        addExecuteSummaries(response.getResponse(), /*delta_mode=*/true);
    response.setEncodeType(encode_type);
    if (blocks.empty())
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(response.getResponse());
        }
        return;
    }

    if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            chunk_codec_stream->encode(block, 0, block.rows());
            blocks.pop_back();
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }
    else /// passthrough data to a TiDB node
    {
        Int64 current_records_num = 0;
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            size_t rows = block.rows();
            for (size_t row_index = 0; row_index < rows;)
            {
                if (current_records_num >= records_per_chunk)
                {
                    response.addChunk(chunk_codec_stream->getString());
                    chunk_codec_stream->clear();
                    current_records_num = 0;
                }
                const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
                chunk_codec_stream->encode(block, row_index, upper);
                current_records_num += (upper - row_index);
                row_index = upper;
            }
            blocks.pop_back();
        }

        if (current_records_num > 0)
        {
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }

    assert(blocks.empty());
    rows_in_blocks = 0;
    writer->write(response.getResponse());
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::asyncWrite(Block && block)
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
void StreamingDAGResponseWriter<StreamWriterPtr>::asyncFinishWrite()
{
    asyncEncodeThenWriteBlocks();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::asyncEncodeThenWriteBlocks()
{
    if (blocks.empty())
        return;

    TrackedMppDataPacket tracked_packet;
    {
        TrackedSelectResp response;
        response.setEncodeType(encode_type);
        if (encode_type == tipb::EncodeType::TypeCHBlock)
        {
            /// passthrough data to a non-TiFlash node, like sending data to TiSpark
            while (!blocks.empty())
            {
                const auto & block = blocks.back();
                chunk_codec_stream->encode(block, 0, block.rows());
                blocks.pop_back();
                response.addChunk(chunk_codec_stream->getString());
                chunk_codec_stream->clear();
            }
        }
        else /// passthrough data to a TiDB node
        {
            Int64 current_records_num = 0;
            while (!blocks.empty())
            {
                const auto & block = blocks.back();
                size_t rows = block.rows();
                for (size_t row_index = 0; row_index < rows;)
                {
                    if (current_records_num >= records_per_chunk)
                    {
                        response.addChunk(chunk_codec_stream->getString());
                        chunk_codec_stream->clear();
                        current_records_num = 0;
                    }
                    const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
                    chunk_codec_stream->encode(block, row_index, upper);
                    current_records_num += (upper - row_index);
                    row_index = upper;
                }
                blocks.pop_back();
            }

            if (current_records_num > 0)
            {
                response.addChunk(chunk_codec_stream->getString());
                chunk_codec_stream->clear();
            }
        }

        assert(blocks.empty());
        rows_in_blocks = 0;

        tracked_packet.serializeByResponse(response.getResponse());
    }

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
bool StreamingDAGResponseWriter<StreamWriterPtr>::asyncIsReady()
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

template class StreamingDAGResponseWriter<StreamWriterPtr>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr>;
} // namespace DB
