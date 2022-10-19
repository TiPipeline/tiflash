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
    std::deque<PipelineTaskPtr> cpu_event_queue;

    std::vector<EventLoopPtr> cpu_loops;

    LoggerPtr logger = Logger::get("event loop pool");

    friend class EventLoop;
    friend class IOPoller;
};
} // namespace DB
