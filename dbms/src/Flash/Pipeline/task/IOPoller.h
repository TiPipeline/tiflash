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

#include <memory>
#include <thread>

namespace DB
{
class EventLoopPool;

class IOPoller
{
public:
    explicit IOPoller(EventLoopPool & pool_);
    void finish();
    void submit(PipelineTaskPtr && task);
    void submit(std::list<PipelineTaskPtr> & tasks);
    ~IOPoller();
private:
    void ioModeLoop();
    bool handleIOModeTask(std::vector<PipelineTaskPtr> & ready_tasks, PipelineTaskPtr && task);
private:
    mutable std::mutex mutex;
    std::condition_variable cond;

    EventLoopPool & pool;

    std::list<PipelineTaskPtr> blocked_tasks;
    std::thread io_thread;

    std::atomic<bool> is_shutdown{false};

    LoggerPtr logger = Logger::get("IOPoller");
};
} // namespace DB
