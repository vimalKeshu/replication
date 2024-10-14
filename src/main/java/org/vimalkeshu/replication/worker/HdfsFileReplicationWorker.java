package org.vimalkeshu.replication.worker;

import lombok.Getter;
import org.vimalkeshu.replication.common.grpc.messages.TaskInfo;
import org.vimalkeshu.replication.common.grpc.messages.TaskStatus;
import org.vimalkeshu.replication.common.grpc.services.MasterGrpc;

import java.util.Objects;

@Getter
public class HdfsFileReplicationWorker implements Runnable {
    private final String workerId;
    private final TaskInfo taskInfo;
    private final MasterGrpc.MasterBlockingStub masterClient;
    private TaskStatus status = TaskStatus.STARTED;
    private final int retry = 3;

    public HdfsFileReplicationWorker(String workerId,
                                     TaskInfo taskInfo,
                                     MasterGrpc.MasterBlockingStub masterClient) {
        this.workerId = workerId;
        this.taskInfo = taskInfo;
        this.masterClient = masterClient;
    }

    @Override
    public void run() {
        this.status = TaskStatus.RUNNING;

        try{
            int count = 0;
            boolean isSucceeded = false;
            while (count < this.retry && !isSucceeded) {
                try {
                    System.out.println("Finished the task: "+ taskInfo.toString() + " by worker: "+ workerId);
                    isSucceeded = true;
                    this.status = TaskStatus.COMPLETED;
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                    this.status = TaskStatus.FAILED;
                }
                count++;
            }
        } catch (Exception ex) {
            this.status = TaskStatus.FAILED;
        }

        // update task status to master.
        if (this.status != TaskStatus.COMPLETED) {
            this.masterClient.taskStatusUpdate(TaskInfo.newBuilder(taskInfo).setStatus(TaskStatus.FAILED).build());
            System.out.println("Replication failed for task: "+this.taskInfo.toString());
        } else {
            this.masterClient.taskStatusUpdate(TaskInfo.newBuilder(taskInfo).setStatus(TaskStatus.COMPLETED).build());
            System.out.println("Replication completed for task: "+ this.taskInfo);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HdfsFileReplicationWorker that = (HdfsFileReplicationWorker) o;
        return Objects.equals(taskInfo.getTask().getTaskId(), that.taskInfo.getTask().getTaskId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskInfo.getTask().getTaskId());
    }
}
