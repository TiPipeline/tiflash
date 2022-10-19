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

#include <Flash/Pipeline/dag/PipelineEvent.h>

#include <atomic>
#include <mutex>
#include <list>

namespace DB
{
enum class PipelineEventQueueStatus
{
    running,
    cancelled,
    finished,
};

class PipelineEventQueue
{
public:
    void submit(PipelineEvent && event);

    void submitFirst(PipelineEvent && event);

    PipelineEventQueueStatus pop(PipelineEvent & event);

    void finish();

    void cancel();

private:
    mutable std::mutex mutex;
    std::condition_variable cond;
    std::list<PipelineEvent> queue;

    std::atomic<PipelineEventQueueStatus> status{PipelineEventQueueStatus::running};
};
using PipelineEventQueuePtr = std::shared_ptr<PipelineEventQueue>;
} // namespace DB
