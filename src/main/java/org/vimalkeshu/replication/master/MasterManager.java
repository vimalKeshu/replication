package org.vimalkeshu.replication.master;

import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import org.vimalkeshu.replication.common.grpc.messages.*;
import org.vimalkeshu.replication.common.grpc.services.WorkerGrpc;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class MasterManager {
    private final Map<String, WorkerGrpc.WorkerBlockingStub> workers = new ConcurrentHashMap<>();
    private final Queue<TaskInfo> taskInfoQueue = new ConcurrentLinkedQueue<>();
    private final Map<String, Map<String, TaskInfo>> taskTracker = new ConcurrentHashMap<>();
    private final Map<String, Job> jobTracker = new ConcurrentHashMap<>();
    public final int totalRetry = 3;

    public void registerWorker(WorkerInfo workerInfo) {
        try{
            WorkerGrpc.WorkerBlockingStub worker = WorkerGrpc.newBlockingStub(ManagedChannelBuilder
                    .forAddress(workerInfo.getHost(), workerInfo.getPort())
                    .usePlaintext()
                    .build());
            workers.put(workerInfo.getId(), worker);
            System.out.println(workerInfo + " is registered.");
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            throw ex;
        }
    }

    public void updateTaskStatus(TaskInfo taskInfo) {
        if (taskTracker.containsKey(taskInfo.getJobId())
                && taskTracker.get(taskInfo.getJobId()).containsKey(taskInfo.getTask().getTaskId())) {
            if (taskInfo.getStatus() == TaskStatus.FAILED
                    && taskInfo.getAttempt() < this.totalRetry) {
                this.getTaskInfoQueue()
                        .add(TaskInfo
                                .newBuilder(taskInfo)
                                .setStatus(TaskStatus.PENDING)
                                .setAttempt(taskInfo.getAttempt() + 1)
                                .build());
            } else {
                taskTracker.get(taskInfo.getJobId()).put(taskInfo.getTask().getTaskId(), taskInfo);
            }
        }
    }

    public JobStatusInfo submitJob(Job job) {
        String jobId = UUID.randomUUID().toString();
        this.jobTracker.put(jobId, Job.newBuilder(job).setJobStatus(JobStatus.running).build());
        this.taskTracker.put(jobId, new ConcurrentHashMap<>());

        for (Task task: job.getTasksList()) {
            TaskInfo taskInfo = TaskInfo
                    .newBuilder()
                    .setJobId(jobId)
                    .setTask(task)
                    .setStatus(TaskStatus.PENDING)
                    .setAttempt(1)
                    .build();

            this.taskTracker
                    .get(jobId)
                    .put(task.getTaskId(), taskInfo);

            this.taskInfoQueue.add(taskInfo);
        }

        return JobStatusInfo
                .newBuilder()
                .setJobId(jobId)
                .setJobStatus(JobStatus.running)
                .build();
    }

    public JobStatusInfo getJobStatus(JobIdInfo jobIdInfo) {
        return updateJobStatus(jobIdInfo);
    }

    public JobStatusInfo updateJobStatus(JobIdInfo jobIdInfo) {
        JobStatus j1 = JobStatus.running;
        Job job = this.jobTracker.getOrDefault(jobIdInfo.getJobId(), null);

        if (job == null) j1 = JobStatus.unknown;
        else if (job.getJobStatus() == JobStatus.failed) j1 = JobStatus.failed;
        else {
            int successCount = 0;
            int failedCount = 0;

            for (Task task: job.getTasksList()) {
                TaskInfo t1 = this.taskTracker.get(jobIdInfo.getJobId()).getOrDefault(task.getTaskId(), null);
                if (t1 == null) throw new RuntimeException("Task, "+task+", is missing from the record !!!");

                if (t1.getStatus() == TaskStatus.FAILED && t1.getAttempt() >= this.totalRetry) failedCount++;
                else if (t1.getStatus() == TaskStatus.COMPLETED) successCount++;
            }

            if (successCount == job.getTasksCount()) j1 = JobStatus.completed;
            else if (failedCount == job.getTasksCount()) j1 = JobStatus.failed;
            else if ((failedCount + successCount) == job.getTasksCount()) j1 = JobStatus.failed;

            // remove job if it has processed all tasks (completed or failed).
            if (j1 == JobStatus.completed || j1 == JobStatus.failed) {
                this.jobTracker.put(jobIdInfo.getJobId(),
                        Job.newBuilder(job).setJobStatus(j1).build());
                this.taskTracker.remove(jobIdInfo.getJobId());
            }
        }

        return JobStatusInfo
                .newBuilder()
                .setJobId(jobIdInfo.getJobId())
                .setJobStatus(j1)
                .build();
    }

}
