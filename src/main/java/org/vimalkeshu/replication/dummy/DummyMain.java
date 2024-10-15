package org.vimalkeshu.replication.dummy;

import io.grpc.ManagedChannelBuilder;
import org.vimalkeshu.replication.common.grpc.messages.*;
import org.vimalkeshu.replication.common.grpc.services.MasterGrpc;

import java.util.ArrayList;
import java.util.List;

public class DummyMain {

    public static void main(String[] args) {
        // get args
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);

        MasterGrpc.MasterBlockingStub masterClient = MasterGrpc
                .newBlockingStub(ManagedChannelBuilder
                        .forAddress(masterHost, masterPort)
                        .usePlaintext()
                        .build());

        List<Task> tasks = new ArrayList<>();
        for (int i=0; i<1000; i++) {
            Task t = Task
                    .newBuilder()
                    .setSourceCatalog("minio_nvme")
                    .setSource("s3a://datalake/db/table")
                    .setTargetCatalog("gcs")
                    .setTarget("gs://spdb-mno-landing/db/table")
                    .setTaskId("file_"+(i+1)+".parquet")
                    .setReplicationType("delta")
                    .build();
            tasks.add(t);
        }

        Job job = Job.newBuilder().addAllTasks(tasks).build();

        JobStatusInfo jobStatusInfo = masterClient.submitJob(job);
        System.out.println(jobStatusInfo);
        while (true) {
            if (jobStatusInfo.getJobStatus() == JobStatus.completed ||
                    jobStatusInfo.getJobStatus() == JobStatus.failed) break;
            jobStatusInfo = masterClient.jobStatus(JobIdInfo.newBuilder().setJobId(jobStatusInfo.getJobId()).build());
            System.out.println(jobStatusInfo);
            try {Thread.sleep(1000);} catch (Exception ignored){}
        }
    }
}
