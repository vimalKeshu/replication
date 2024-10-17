package org.vimalkeshu.replication.dummy;

import io.grpc.ManagedChannelBuilder;
import org.vimalkeshu.replication.grpc.messages.*;
import org.vimalkeshu.replication.grpc.services.MasterGrpc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DummyMain {


    public static void testCopyFiles (String masterHost, int masterPort) throws Exception {
        Path sourceDirPath = Files.createTempDirectory("testDir1");
        Path targetDirPath = Files.createTempDirectory("testDir2");
        Charset utf8 = StandardCharsets.UTF_8;
        List<String> lines = Arrays.asList("1st line", "2nd line");
        List<Task> tasks = new ArrayList<>();
        for (int i=1; i<=10; i++) {
            String fileName = "file_"+i+".txt";
            Path filepath = Paths.get(sourceDirPath.toString(),fileName);
            Files.write(filepath, lines, utf8);
            Task t = Task
                    .newBuilder()
                    .setSourceCatalog("local")
                    .setSource("file://"+ sourceDirPath)
                    .setTargetCatalog("local")
                    .setTarget("file://"+targetDirPath)
                    .setTaskId(fileName)
                    .setDelta(fileName)
                    .setReplicationType("delta")
                    .build();
            tasks.add(t);
            System.out.println(filepath);
            System.out.println(t);
            System.out.println("------------------------");
        }

        MasterGrpc.MasterBlockingStub masterClient = MasterGrpc
                .newBlockingStub(ManagedChannelBuilder
                        .forAddress(masterHost, masterPort)
                        .usePlaintext()
                        .build());

        Job job = Job.newBuilder().addAllTasks(tasks).build();
        JobStatusInfo jobStatusInfo = masterClient.submitJob(job);
        System.out.println(jobStatusInfo);
        while (jobStatusInfo.getJobStatus() != JobStatus.completed &&
                jobStatusInfo.getJobStatus() != JobStatus.failed) {
            jobStatusInfo = masterClient.jobStatus(JobIdInfo.newBuilder().setJobId(jobStatusInfo.getJobId()).build());
            System.out.println(jobStatusInfo);
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
        }
    }

    public static void testDummyTasks(String masterHost, int masterPort) throws Exception {
        MasterGrpc.MasterBlockingStub masterClient = MasterGrpc
                .newBlockingStub(ManagedChannelBuilder
                        .forAddress(masterHost, masterPort)
                        .usePlaintext()
                        .build());

        List<Task> tasks = new ArrayList<>();
        for (int i=0; i<10000; i++) {
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
        while (jobStatusInfo.getJobStatus() != JobStatus.completed &&
                jobStatusInfo.getJobStatus() != JobStatus.failed) {
            jobStatusInfo = masterClient.jobStatus(JobIdInfo.newBuilder().setJobId(jobStatusInfo.getJobId()).build());
            System.out.println(jobStatusInfo);
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // get args
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        testDummyTasks(masterHost, masterPort);
    }
}
