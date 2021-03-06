/**
 * Tests that $setWindowFields stage reports memory footprint per function when explain is run
 * with verbosities "executionStats" and "allPlansExecution". Also tests that the explain output
 * includes a metric for peak memory usage across the entire stage, including each individual
 * function as well as any other internal state.
 *
 * @tags: [assumes_against_mongod_not_mongos]
 */
(function() {
"use strict";

load("jstests/libs/analyze_plan.js");  // For getAggPlanStages().

const coll = db[jsTestName()];
coll.drop();
const bigStr = Array(1025).toString();  // 1KB of ','
const nDocs = 1000;
const nPartitions = 50;
const docSize = 8 + 8 + 1024;

const featureEnabled =
    assert.commandWorked(db.adminCommand({getParameter: 1, featureFlagWindowFunctions: 1}))
        .featureFlagWindowFunctions.value;
if (!featureEnabled) {
    jsTestLog("Skipping test because the window function feature flag is disabled");
    return;
}

let bulk = coll.initializeUnorderedBulkOp();
for (let i = 1; i <= nDocs; i++) {
    bulk.insert({_id: i, key: i % nPartitions, bigStr: bigStr});
}
assert.commandWorked(bulk.execute());

/**
 * Checks that the execution stats in the explain output for a $setWindowFields stage are as
 * expected.
 * - 'stages' is an array of the explain output of $setWindowFields stages.
 * - 'expectedFunctionMemUsages' is used to check the memory footprint stats for each function.
 * - 'expectedTotalMemUsage' is used to check the peak memory footprint for the entire stage.
 * - 'verbosity' indicates the explain verbosity used.
 */
function checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotalMemUsage, verbosity) {
    const stages =
        getAggPlanStages(coll.explain(verbosity).aggregate(pipeline), "$_internalSetWindowFields");
    for (let stage of stages) {
        assert(stage.hasOwnProperty("$_internalSetWindowFields"), stage);

        if (verbosity === "executionStats" || verbosity === "allPlansExecution") {
            assert(stage.hasOwnProperty("maxFunctionMemoryUsageBytes"), stage);
            const maxFunctionMemUsages = stage["maxFunctionMemoryUsageBytes"];
            for (let field of Object.keys(maxFunctionMemUsages)) {
                // Ensures that the expected functions are all included and the corresponding
                // memory usage is in a reasonable range.
                if (expectedFunctionMemUsages.hasOwnProperty(field)) {
                    assert.gt(maxFunctionMemUsages[field],
                              expectedFunctionMemUsages[field],
                              "mismatch for function '" + field + "': " + tojson(stage));
                    assert.lt(maxFunctionMemUsages[field],
                              2 * expectedFunctionMemUsages[field],
                              "mismatch for function '" + field + "': " + tojson(stage));
                }
            }
            assert.gt(stage["maxTotalMemoryUsageBytes"],
                      expectedTotalMemUsage,
                      "Incorrect total mem usage: " + tojson(stage));
            assert.lt(stage["maxTotalMemoryUsageBytes"],
                      2 * expectedTotalMemUsage,
                      "Incorrect total mem usage: " + tojson(stage));
        } else {
            assert(!stage.hasOwnProperty("maxFunctionMemoryUsageBytes"), stage);
            assert(!stage.hasOwnProperty("maxTotalMemoryUsageBytes"), stage);
        }
    }
}

(function testQueryPlannerVerbosity() {
    const pipeline = [
        {
            $setWindowFields:
                {output: {count: {$sum: 1}, push: {$push: "$bigStr"}, set: {$addToSet: "$bigStr"}}}
        },
    ];
    const stages = getAggPlanStages(coll.explain("queryPlanner").aggregate(pipeline),
                                    "$_internalSetWindowFields");
    checkExplainResult(stages, {}, 0, "queryPlanner");
})();

(function testUnboundedMemUsage() {
    let pipeline = [
        {
            $setWindowFields:
                {output: {count: {$sum: 1}, push: {$push: "$bigStr"}, set: {$addToSet: "$bigStr"}}}
        },
    ];

    // The $setWindowFields stage "streams" one partition at a time, so there's only one instance of
    // each function. For the default [unbounded, unbounded] window type, each function uses memory
    // usage comparable to it's $group counterpart.
    let expectedFunctionMemUsages = {
        count: 60,
        push: nDocs * 1024,
        set: 1024,
    };

    // The total mem usage for unbounded windows is the total from each function as well as the size
    // of all documents in the partition.
    let expectedTotal = nDocs * docSize;
    for (let func in expectedFunctionMemUsages) {
        expectedTotal += expectedFunctionMemUsages[func];
    }

    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "executionStats");
    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "allPlansExecution");

    // Test that the memory footprint is reduced with partitioning.
    pipeline = [
        {
            $setWindowFields: {
                partitionBy: "$key",
                output: {count: {$sum: 1}, push: {$push: "$bigStr"}, set: {$addToSet: "$bigStr"}}
            }
        },
    ];
    expectedFunctionMemUsages = {
        count: 60,
        push: (nDocs / nPartitions) * 1024,
        set: 1024,
    };
    expectedTotal = (nDocs / nPartitions) * docSize;
    for (let func in expectedFunctionMemUsages) {
        expectedTotal += expectedFunctionMemUsages[func];
    }

    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "executionStats");
    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "allPlansExecution");
})();

(function testSlidingWindowMemUsage() {
    const windowSize = 10;
    let pipeline = [
        {
            $setWindowFields: {
                sortBy: {_id: 1},
                output: {runningSum: {$sum: "$_id", window: {documents: [-5, 4]}}}
            }
        },
    ];
    const expectedFunctionMemUsages = {
        runningSum: windowSize * 16 +
            160,  // 10x64-bit integer values per window, and 160 for the $sum state.
    };

    // TODO SERVER-55786: Fix memory tracking in PartitionIterator when documents are
    // released from the in-memory cache. This should be proportional to the size of the window not
    // the number of documents in the partition.
    let expectedTotal = nDocs * docSize;
    for (let func in expectedFunctionMemUsages) {
        expectedTotal += expectedFunctionMemUsages[func];
    }

    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "executionStats");
    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "allPlansExecution");

    // Adding partitioning doesn't change the peak memory usage.
    pipeline = [
        {
            $setWindowFields: {
                partitionBy: "$key",
                sortBy: {_id: 1},
                output: {runningSum: {$sum: "$_id", window: {documents: [-5, 4]}}}
            }
        },
    ];

    // TODO SERVER-55786: This should not be needed once the memory tracking is fixed in the
    // iterator. For now, the iterator handles the tracking correctly across partition boundaries so
    // we need to adjust the expected total.
    expectedTotal = (nDocs / nPartitions) * docSize;
    for (let func in expectedFunctionMemUsages) {
        expectedTotal += expectedFunctionMemUsages[func];
    }
    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "executionStats");
    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "allPlansExecution");
})();

(function testRangeBasedWindowMemUsage() {
    const maxDocsInWindow = 20;
    let pipeline = [
        {
            $setWindowFields: {
                sortBy: {_id: 1},
                output: {pushArray: {$push: "$bigStr", window: {range: [-10, 9]}}}
            }
        },
    ];
    // The memory usage is doubled since both the executor and the function state have copies of the
    // large string.
    const expectedFunctionMemUsages = {pushArray: 1024 * maxDocsInWindow * 2};

    // TODO SERVER-55786: Fix memory tracking in PartitionIterator when documents are
    // released from the in-memory cache. This should be proportional to the size of the window not
    // the number of documents in the partition.
    let expectedTotal = nDocs * docSize;
    for (let func in expectedFunctionMemUsages) {
        expectedTotal += expectedFunctionMemUsages[func];
    }

    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "executionStats");
    checkExplainResult(pipeline, expectedFunctionMemUsages, expectedTotal, "allPlansExecution");
})();
}());
