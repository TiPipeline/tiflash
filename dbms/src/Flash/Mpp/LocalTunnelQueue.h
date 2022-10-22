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

#pragma once

#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Common/MPMCQueue.h>

#include <atomic>
#include <mutex>
#include <deque>

namespace DB
{
enum class LocalTunnelQueueStatus
{
    running,
    cancelled,
    finished,
};

class LocalTunnelQueue
{
public:
    explicit LocalTunnelQueue(size_t queue_size_)
        : queue_size(queue_size_)
        , queue(MPMCQueue<TrackedMppDataPacketPtr>(queue_size))
    {}

    MPMCQueueResult tryPush(const TrackedMppDataPacketPtr & packet)
    {
        assert(packet);
        if (is_counting.load())
        {
            if (cur_count.fetch_add(1) >= queue_size)
            {
                --cur_count;
                return MPMCQueueResult::FULL;
            }
            auto res = queue.tryPush(packet);
            if (res != MPMCQueueResult::OK)
                --cur_count;
            return res;
        }
        else
        {
            return queue.tryPush(packet);
        }
    }
    MPMCQueueResult tryPop(TrackedMppDataPacketPtr & packet)
    {
        assert(!packet);
        if (is_counting.load())
        {
            if (cur_count.fetch_sub(1) <= 0)
            {
                ++cur_count;
                return MPMCQueueResult::EMPTY;
            }
            auto res = queue.tryPop(packet);
            if (res != MPMCQueueResult::OK)
                ++cur_count;
            return res;
        }
        else
        {
            return queue.tryPop(packet);
        }
    }

    bool finish()
    {
        is_counting = false;
        return queue.finish();
    }
    bool cancelWith(const String & reason)
    {
        is_counting = false;
        return queue.cancelWith(reason);
    }

    const String & getCancelReason() const
    {
        return queue.getCancelReason();
    }

private:
    const int32_t queue_size;
    MPMCQueue<TrackedMppDataPacketPtr> queue;

    std::atomic_int32_t cur_count{0};
    std::atomic_bool is_counting{true};
};
} // namespace DB
