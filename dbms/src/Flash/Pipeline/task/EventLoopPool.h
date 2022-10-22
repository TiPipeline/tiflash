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

#include <Common/Logger.h>
#include <Flash/Pipeline/task/PipelineTask.h>
#include <Flash/Pipeline/task/IOPoller.h>

#include <array>
#include <memory>
#include <thread>

namespace DB
{
class EventLoop
{
public:
    EventLoop(
        size_t loop_id_,
        EventLoopPool & pool_);
    ~EventLoop();
private:
    void handleCpuModeTask(PipelineTaskPtr && task);
    void cpuModeLoop();
private:
    size_t loop_id;
    EventLoopPool & pool;
    std::thread cpu_thread;
    LoggerPtr logger = Logger::get(fmt::format("event loop {}", loop_id));
};
using EventLoopPtr = std::unique_ptr<EventLoop>;

struct WorkGroup
{
    explicit WorkGroup(size_t priority_)
        : priority(priority_)
        , origin_priority(priority_)
    {
        assert(priority_ > 0);
    }
    
    // priority
    bool isZeroPriority() { return 0 == priority; }
    void resetPriority() { priority = origin_priority; }

    bool empty() { return cpu_event_queue.empty(); }

    void pop(PipelineTaskPtr & task)
    {
        if (priority > 0)
            --priority;
        assert(!cpu_event_queue.empty());
        task = std::move(cpu_event_queue.front());
        assert(task);
        cpu_event_queue.pop_front();
    }

    void push(PipelineTaskPtr && task)
    {
        cpu_event_queue.emplace_back(std::move(task));
    }

    std::deque<PipelineTaskPtr> cpu_event_queue;
    size_t priority;
    size_t origin_priority;
};

struct WorkGroups
{
    bool empty() { return work_groups[0].empty() && work_groups[1].empty(); }

    void submit(PipelineTaskPtr && task)
    {
        work_groups[task->groupId()].push(std::move(task));
    }

    void pop(PipelineTaskPtr & task)
    {
        assert(!empty());
        if (work_groups[0].empty())
        {
            work_groups[1].pop(task);
        }
        else if (work_groups[1].empty())
        {
            work_groups[0].pop(task);
        }
        else
        {
            while (true)
            {
                if (work_groups[0].isZeroPriority() && work_groups[1].isZeroPriority())
                {
                    work_groups[0].resetPriority();
                    work_groups[1].resetPriority();
                    continue;
                }
                else if (work_groups[0].priority > work_groups[1].priority)
                {
                    work_groups[0].pop(task);
                }
                else
                {
                    work_groups[1].pop(task);
                }
                return;
            }
        }
    }

    std::array<WorkGroup, 2> work_groups{WorkGroup(1), WorkGroup(2)};
};

class EventLoopPool
{
public:
    explicit EventLoopPool(size_t loop_num);

    void finish();

    void submit(std::vector<PipelineTaskPtr> & tasks);

    size_t concurrency() const { return cpu_loops.size(); }

    ~EventLoopPool();

private:
    void submitCPU(PipelineTaskPtr && task);
    void submitCPU(std::vector<PipelineTaskPtr> & tasks);

    bool popTask(PipelineTaskPtr & task);

private:
    IOPoller io_poller;

    mutable std::mutex global_mutex;
    std::condition_variable cv;
    bool is_closed = false;
    WorkGroups work_groups;

    std::vector<EventLoopPtr> cpu_loops;

    LoggerPtr logger = Logger::get("event loop pool");

    friend class EventLoop;
    friend class IOPoller;
};
} // namespace DB
