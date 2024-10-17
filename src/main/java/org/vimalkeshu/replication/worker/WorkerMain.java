package org.vimalkeshu.replication.worker;

import io.grpc.ManagedChannelBuilder;
import org.vimalkeshu.replication.common.JobService;
import org.vimalkeshu.replication.grpc.services.MasterGrpc;

import java.io.IOException;

public class WorkerMain {

    public static MasterGrpc.MasterBlockingStub createMasterServiceClient(String host, int port) {
        MasterGrpc.MasterBlockingStub masterBlockingStub = MasterGrpc
                .newBlockingStub(ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build());

        return masterBlockingStub;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // get args
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        String workerHost = args[2];
        int workerPort = Integer.parseInt(args[3]);
        String workerId = "Worker_"+workerHost+"_"+workerPort;
        JobService jobService = new JobService(100);
        // initialize master service client
        MasterGrpc.MasterBlockingStub masterClient = createMasterServiceClient(masterHost, masterPort);

        // start worker service
        WorkerService workerService = new WorkerService(
                workerId,
                workerHost,
                workerPort,
                jobService,
                masterClient);
        workerService.start();
        workerService.blockUntilShutdown();
    }
}
