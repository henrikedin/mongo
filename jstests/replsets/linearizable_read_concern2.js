/*
 * This test creates a 3 node replica set. The test first sends
 * a regular linearizable read command which should succeed. Then the test
 * examines linearizable read parsing abilities by sending a linearizable
 * read command to a secondary and then to the primary with an 'afterOpTime'
 * field, both of which should fail. The test then starts to test the actual
 * functionality of linearizable reads by creating a network partition between the primary
 * and the other two nodes and then sending in a linearizable read command.
 * Finally we test whether the linearizable read command will block forever
 * by issuing a linearizable read command in another thread on the still
 * partitioned primary and then making the primary step down in the main
 * thread after finding the issued noop. The secondary thread should throw
 * an exception and exit.
 */
load('jstests/replsets/rslib.js');
load('jstests/libs/parallelTester.js');
load('jstests/libs/write_concern_util.js');
(function() {
'use strict';
var send_linearizable_read = function() {
    // The primary will step down and throw an exception, which is expected.
    db.getMongo().setSecondaryOk();
    var coll = db.getSiblingDB("test").foo;
    jsTestLog('Sending in linearizable read in secondary thread');
    // 'hello' ensures that the following command fails (and returns a response rather than
    // an exception) before its connection is cut because of the primary step down. Refer to
    // SERVER-24574.
    //assert.commandWorked(coll.runCommand({hello: 1, hangUpOnStepDown: false}));

    coll.runCommand({'find': 'foo'});

    assert.commandWorked(db.getMongo().adminCommand({
        waitForFailPoint: "hangAfterReconfigOnDrainComplete",
        timesEntered: 1,
        maxTimeMS: 5 * 60 * 1000
    }));

    try {
        var run = true;
        //while (run) {
        print("starting attempt");
        //
        var res = coll.runCommand({'find': 'foo', readConcern: {level: "linearizable"}, maxTimeMS: 60000});
        print("attempt ended with")
        print(tojson(res));
        run = res.ok == 0;
    }
    finally {
        assert.commandWorked(db.getMongo().adminCommand({
            configureFailPoint: "hangAfterReconfigOnDrainComplete",
            mode: "off"
        }));
    }
    
    //}

    

    print("succeded with find!, exiting parallel shell");
    
    // assert.commandFailedWithCode(
    //     coll.runCommand({'find': 'foo', readConcern: {level: "linearizable"}, maxTimeMS: 60000}),
    //     ErrorCodes.InterruptedDueToReplStateChange);
};

var num_nodes = 3;
var name = 'linearizable_read_concern';
var replTest = new ReplSetTest({name: name, nodes: num_nodes, useBridge: true});
var config = replTest.getReplSetConfig();

// Increased election timeout to avoid having the primary step down while we are
// testing linearizable functionality on an isolated primary.
config.settings = {
    electionTimeoutMillis: 60000
};

replTest.startSet();
replTest.initiate(config);

// Without a sync source the heartbeat interval will be half of the election timeout, 30
// seconds. It thus will take almost 30 seconds for the secondaries to set the primary as
// their sync source and begin replicating.
replTest.awaitReplication();
var primary = replTest.getPrimary();
var secondaries = replTest.getSecondaries();

// Do a write to have something to read.
assert.commandWorked(primary.getDB("test").foo.insert(
    {"number": 7}, {"writeConcern": {"w": "majority", "wtimeout": ReplSetTest.kDefaultTimeoutMS}}));

primary = replTest.getPrimary();

jsTestLog("Starting linearizablility testing");

jsTestLog("Testing to make sure linearizable read command does not block forever.");

assert.commandWorked(secondaries[0].adminCommand({
    configureFailPoint: "hangAfterReconfigOnDrainComplete",
    mode: "alwaysOn"
}));

var parallelShell = startParallelShell(send_linearizable_read, secondaries[0].port);

sleep(1000);

jsTestLog("Making Seconday step up");

assert.commandWorked(
    secondaries[0].adminCommand({"replSetStepUp": 1}));

sleep(20000);

assert.commandWorked(secondaries[0].adminCommand({
    configureFailPoint: "hangAfterReconfigOnDrainComplete",
    mode: "off"
}));

//while (true) {



// Sending a linearizable read implicitly replicates a noop to the secondaries. We need to find
// the most recently issued noop to ensure that we call stepdown during the recently
// issued linearizable read and not before the read (in the separate thread) has been called.

parallelShell();
//}
replTest.stopSet();
}());
