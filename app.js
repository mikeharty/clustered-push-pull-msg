var cluster = require('cluster'),
    zmq = require('zmq'),
    _ = require('underscore');

// Master
if(cluster.isMaster) {
    var pusher = zmq.socket('push').bind('ipc://job-delegator.ipc'),
        puller = zmq.socket('pull').bind('ipc://backchannel.ipc'),
        readyWorkers = 0,
        workerJobsCompleted = {},
        jobsCompleted = 0;

    // Handle messages from workers
    puller.on('message', function(data) {
        var message = JSON.parse(data.toString());
        // Worker online message
        if(message.type == 'ready') {
            // Log online workers
            console.log(message.pid + ' came online');
            // Initialize worker
            workerJobsCompleted[message.pid] = 0;
            // Tally ready workers
            readyWorkers++;
            // When all workers are online, push jobs
            if(readyWorkers == 5) {
                for(var i = 0; i < 10000; i++) {
                    var job = { jobId: i+1, msg: 'Do work!', timestamp: Date.now() };
                    pusher.send(JSON.stringify(job));
                }
            }
        }
        // Job completed message
        else {
            // Tally jobs completed
            jobsCompleted++;
            workerJobsCompleted[message.pid]++;
            // Log pid, job id, and timestamp
            console.log('Worker ' + message.pid + ' completed job #'+message.jobId + ' at ' + message.timestamp);
            // When all jobs are completed, log results
            if(jobsCompleted == 10000) {
                _.each(workerJobsCompleted, function(workerJobsCompleted, worker) {
                    console.log(worker + ' completed ' + workerJobsCompleted + ' jobs');
                });
                process.exit(0);
            }
        }
    });

    // Create workers
    for(var i = 0; i < 5; i++) {
        cluster.fork();
    }
}
// Worker
else {
    pusher = zmq.socket('push').connect('ipc://backchannel.ipc');
    puller = zmq.socket('pull').connect('ipc://job-delegator.ipc');

    // Report that worker is online
    var onlineMessage = { type: 'ready', pid: process.pid };
    pusher.send(JSON.stringify(onlineMessage));

    // When job is received, send response
    puller.on('message', function(data) {
        var message = JSON.parse(data.toString());
        var response = { type: 'result', pid: process.pid, jobId: message.jobId, timestamp: Date.now() };
        pusher.send(JSON.stringify(response));
    });
}

