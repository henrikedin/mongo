"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

/**
 * Repeatedly creates a regular collection and a time-series collection on the same namespace.
 * Validates that we never manage to have both a Collection and View created on the same namespace.
 *
 * @tags: [
 *   assumes_no_implicit_collection_creation_after_drop,
 *   does_not_support_stepdowns,
 *   requires_fcv_49,
 * ]
 */
var $config = (function() {
    var data = {prefix: "create_timeseries_collection", supportsTimeseriesCollections: false};

    var states = (function() {
        function getCollectionName(prefix, collName) {
            return prefix + "_" + collName;
        }

        function init(db, collName) {
            this.num = 0;

            if (!TimeseriesTest.timeseriesCollectionsEnabled(db.getMongo())) {
                jsTestLog(
                    "Skipping test because the time-series collection feature flag is disabled");
                return;
            }

            this.supportsTimeseriesCollections = true;
        }

        function createTimeseries(db, collName) {
            if (!this.supportsTimeseriesCollections) {
                return;
            }

            collName = getCollectionName(this.prefix, collName, this.tid);

            const timeFieldName = "time";
            // The view creation code may uassert with code 17399 if Collection already exist.
            assertAlways.commandWorkedOrFailedWithCode(
                db.createCollection(collName, {timeseries: {timeField: timeFieldName}}),
                [ErrorCodes.NamespaceExists, 17399]);
        }

        function createCollection(db, collName) {
            collName = getCollectionName(this.prefix, collName, this.tid);

            assertAlways.commandWorkedOrFailedWithCode(db.createCollection(collName),
                                                       ErrorCodes.NamespaceExists);
        }

        function listCollections(db, collName) {
            collName = getCollectionName(this.prefix, collName, this.tid);
            let res = db.runCommand("listCollections", {filter: {name: collName}});
            assertAlways.commandWorked(res);
            assertAlways.lt(res.cursor.firstBatch.length, 2);
        }

        function drop(db, collName) {
            if (!this.supportsTimeseriesCollections) {
                return;
            }

            collName = getCollectionName(this.prefix, collName, this.tid);

            db.getCollection(collName).drop();
        }

        return {
            init: init,
            createCollection: createCollection,
            createTimeseries: createTimeseries,
            listCollections: listCollections,
            drop: drop
        };
    })();

    var transitions = {
        init: {createCollection: 0.5, createTimeseries: 0.5},
        createCollection: {createCollection: 0.5, listCollections: 0.5},
        createTimeseries: {createTimeseries: 0.5, listCollections: 0.5},
        listCollections: {drop: 1.0},
        drop: {createCollection: 0.5, createTimeseries: 0.5}
    };

    return {
        threadCount: 4,
        iterations: 1000,
        data: data,
        states: states,
        transitions: transitions,
    };
})();
