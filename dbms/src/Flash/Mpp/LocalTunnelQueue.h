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
    explicit LocalTunnelQueue(size_t queue_size_): queue_size(queue_size_) {}

    MPMCQueueResult tryPush(const TrackedMppDataPacketPtr & event)
    {
        switch (status.load())
        {
        case LocalTunnelQueueStatus::running:
        {
            if (cur_count.fetch_add(1) >= queue_size)
            {
                --cur_count;
                return MPMCQueueResult::FULL;
            }
            std::lock_guard lock(mutex);
            queue.push_back(event);
            return MPMCQueueResult::OK;
        }
        case LocalTunnelQueueStatus::cancelled:
            return MPMCQueueResult::CANCELLED;
        case LocalTunnelQueueStatus::finished:
        default:
            return MPMCQueueResult::FINISHED;
        }
    }
    MPMCQueueResult tryPop(TrackedMppDataPacketPtr & event)
    {
        switch (status.load())
        {
        case LocalTunnelQueueStatus::running:
        {
            if (cur_count.fetch_sub(1) <= 0)
            {
                ++cur_count;
                return MPMCQueueResult::EMPTY;
            }
            std::lock_guard lock(mutex);
            if (queue.empty())
            {
                ++cur_count;
                return MPMCQueueResult::EMPTY;
            }
            event = std::move(queue.front());
            queue.pop_front();
            return MPMCQueueResult::OK;
        }
        case LocalTunnelQueueStatus::cancelled:
            return MPMCQueueResult::CANCELLED;
        case LocalTunnelQueueStatus::finished:
        default:
            return MPMCQueueResult::FINISHED;
        }
    }

    bool finish()
    {
        LocalTunnelQueueStatus running_status = LocalTunnelQueueStatus::running;
        return status.compare_exchange_strong(running_status, LocalTunnelQueueStatus::finished);
    }
    bool cancelWith(const String & reason)
    {
        LocalTunnelQueueStatus running_status = LocalTunnelQueueStatus::running;
        if (status.compare_exchange_strong(running_status, LocalTunnelQueueStatus::cancelled))
        {
            std::lock_guard lock(mutex);
            cancel_reason = reason;
            return true;
        }
        else
            return false;
    }

    const String & getCancelReason() const
    {
        std::lock_guard lock(mutex);
        RUNTIME_ASSERT(status == LocalTunnelQueueStatus::cancelled);
        return cancel_reason;
    }

private:
    int32_t queue_size;
    mutable std::mutex mutex;
    std::deque<TrackedMppDataPacketPtr> queue;
    
    std::atomic_int32_t cur_count{0};
    std::atomic<LocalTunnelQueueStatus> status{LocalTunnelQueueStatus::running};
    String cancel_reason;
};
} // namespace DB
