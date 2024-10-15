package org.vimalkeshu.replication.master;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.vimalkeshu.replication.common.JobService;
import org.vimalkeshu.replication.common.grpc.messages.*;
import org.vimalkeshu.replication.common.grpc.services.MasterGrpc;

import java.io.IOException;

public class MasterService {
    private final String masterHost;
    private final int masterPort;
    private final Server server;
    private final MasterManager manager;
    private final JobService taskDistributorScheduler;
    private final TaskDistributorWorker taskDistributorWorker;

    public MasterService(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.server = ServerBuilder.forPort(masterPort)
                .addService(new MasterServiceImpl())
                .build();
        this.manager = new MasterManager();
        this.taskDistributorScheduler = new JobService(1);
        this.taskDistributorWorker = new TaskDistributorWorker(this.manager);
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        this.taskDistributorScheduler.submit(this.taskDistributorWorker);
    }

    public void stop() {
        if (server != null) {
            this.taskDistributorWorker.setStop(true);
            this.taskDistributorScheduler.shutDown();
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private class MasterServiceImpl extends MasterGrpc.MasterImplBase {
        public void workerRegistration(WorkerInfo request, StreamObserver<Empty> responseObserver) {
            manager.registerWorker(request);
            Empty reply = Empty.newBuilder().build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        public void taskStatusUpdate(TaskInfo request, StreamObserver<Empty> responseObserver) {
            manager.updateTaskStatus(request);
            Empty reply = Empty.newBuilder().build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        public void submitJob(Job job, StreamObserver<JobStatusInfo> responseObserver) {
            JobStatusInfo jobStatusInfo = manager.submitJob(job);
            responseObserver.onNext(jobStatusInfo);
            responseObserver.onCompleted();
        }

        public void jobStatus(JobIdInfo jobIdInfo, StreamObserver<JobStatusInfo> responseObserver) {
            JobStatusInfo jobStatusInfo = manager.getJobStatus(jobIdInfo);
            responseObserver.onNext(jobStatusInfo);
            responseObserver.onCompleted();
        }

    }
}
