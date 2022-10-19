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

#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalPartialAggregation.h>
#include <Transforms/AggregateSink.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalPartialAggregation::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);

    Names before_agg_output;
    // set required output for agg funcs's arguments and group by keys.
    for (const auto & aggregate_description : aggregate_descriptions)
    {
        for (const auto & argument_name : aggregate_description.argument_names)
            before_agg_output.push_back(argument_name);
    }
    for (const auto & aggregation_key : aggregation_keys)
    {
        before_agg_output.push_back(aggregation_key);
    }

    before_agg_actions->finalize(before_agg_output);
    child->finalize(before_agg_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_agg_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalPartialAggregation::getSampleBlock() const
{
    return before_agg_actions->getSampleBlock();
}

void PhysicalPartialAggregation::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalPartialAggregation::transform(TransformsPipeline & pipeline, Context & context, size_t concurrency)
{
    child->transform(pipeline, context, concurrency);

    pipeline.transform([&](auto & transforms) {
        transforms->append(std::make_shared<ExpressionTransform>(before_agg_actions));
    });

    Block before_agg_header = pipeline.getHeader();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    size_t max_threads = std::min(concurrency, pipeline.concurrency());
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        max_threads,
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg);
    aggregate_store->init(max_threads, params);

    size_t index = 0;
    pipeline.transform([&](auto & transforms) {
        transforms->setSink(std::make_shared<AggregateSink>(aggregate_store, index % aggregate_store->max_threads));
        ++index;
    });
}
} // namespace DB
