package org.vimalkeshu.replication.worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.vimalkeshu.replication.common.JobService;
import org.vimalkeshu.replication.dummy.HdfsFileReplicationWorker;
import org.vimalkeshu.replication.grpc.messages.Empty;
import org.vimalkeshu.replication.grpc.messages.TaskInfo;
import org.vimalkeshu.replication.grpc.messages.WorkerInfo;
import org.vimalkeshu.replication.grpc.services.MasterGrpc;
import org.vimalkeshu.replication.grpc.services.WorkerGrpc;

import java.io.IOException;

public class WorkerService {
    private final String workerId;
    private final String workerHost;
    private final int workerPort;
    private final MasterGrpc.MasterBlockingStub masterClient;
    private final JobService jobService;
    private final Server server;


    public WorkerService(String workerId,
                         String workerHost,
                         int workerPort,
                         JobService jobService,
                         MasterGrpc.MasterBlockingStub masterClient) {
        this.workerId = workerId;
        this.workerHost = workerHost;
        this.workerPort = workerPort;
        this.jobService = jobService;
        this.masterClient = masterClient;
        this.server = ServerBuilder.forPort(workerPort)
                .addService(new WorkerServiceImpl())
                .build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        // register worker to master
        register();
    }

    public void register() {
        WorkerInfo workerInfo = WorkerInfo
                .newBuilder()
                .setHost(workerHost)
                .setPort(workerPort)
                .setId(workerId)
                .build();
        this.masterClient.workerRegistration(workerInfo);
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    // Await termination on the main thread since the grpc library uses daemon threads.
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private class WorkerServiceImpl extends WorkerGrpc.WorkerImplBase {
        public void taskAssignment(TaskInfo request, StreamObserver<Empty> responseObserver) {
            jobService.submit(new HdfsFileReplicationWorker(workerId, request, masterClient));
            Empty reply = Empty.newBuilder().build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
