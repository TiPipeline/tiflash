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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/Planner.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>

namespace DB
{
Planner::Planner(
    Context & context_,
    const PlanQuerySource & plan_source_)
    : context(context_)
    , plan_source(plan_source_)
    , max_streams(context.getMaxStreams())
    , log(Logger::get(dagContext().log ? dagContext().log->identifier() : ""))
{}

BlockIO Planner::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    executeCreatingSets(pipeline, context, max_streams, log);
    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}

QueryExecutorPtr Planner::pipelineExecute(std::shared_ptr<ProcessListEntry> process_list_entry) const
{
    PhysicalPlan physical_plan{context, log->identifier()};

    physical_plan.build(&plan_source.getDAGRequest());
    physical_plan.outputAndOptimize();

    return std::make_unique<PipelineExecutor>(
        context,
        physical_plan.rootNode(),
        log->identifier(),
        process_list_entry);
}

DAGContext & Planner::dagContext() const
{
    return *context.getDAGContext();
}

void Planner::executeImpl(DAGPipeline & pipeline)
{
    PhysicalPlan physical_plan{context, log->identifier()};

    physical_plan.build(&plan_source.getDAGRequest());
    physical_plan.outputAndOptimize();

    physical_plan.transform(pipeline, context, max_streams);
}
} // namespace DB
