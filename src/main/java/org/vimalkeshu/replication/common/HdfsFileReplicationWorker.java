package org.vimalkeshu.replication.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.vimalkeshu.replication.grpc.messages.Task;
import org.vimalkeshu.replication.grpc.messages.TaskInfo;
import org.vimalkeshu.replication.grpc.messages.TaskStatus;
import org.vimalkeshu.replication.grpc.services.MasterGrpc;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class HdfsFileReplicationWorker implements Runnable {
    //private static final Logger logger = LoggerFactory.getLogger(HdfsFileReplicationWorker.class);
    private final String workerId;
    private final TaskInfo taskInfo;
    private final MasterGrpc.MasterBlockingStub masterClient;
    private TaskStatus status = TaskStatus.STARTED;

    @Override
    public void run() {
        int retry = ReplicationConstant.RETRY_DEFAULT;
        if (this.taskInfo.getTask().getTotalRetry() > ReplicationConstant.RETRY_DEFAULT)
            retry = this.taskInfo.getTask().getTotalRetry();
        int blockSize = ReplicationConstant.HDFS_BLOCK_SIZE;
        if (this.taskInfo.getTask().getBlockSize() > ReplicationConstant.HDFS_BLOCK_SIZE)
            blockSize = this.taskInfo.getTask().getBlockSize();
        this.status = TaskStatus.RUNNING;
        FileSystem sourceFileSystem = null;
        FileSystem targetFileSystem = null;
        Path sourceFilePath = null;
        Path targetFilePath = null;
        TaskInfo t1 = null;

        // copy the file
        try{
            sourceFilePath = new Path(taskInfo.getTask().getSource() + taskInfo.getTask().getDelta());
            sourceFileSystem = HadoopFileSystemOps
                    .getFileSystemInstance(taskInfo.getTask().getSourceCatalog(), taskInfo.getTask().getSource());
            if (!sourceFileSystem.exists(sourceFilePath)) throw new Exception("Source,"+sourceFilePath+", doesn't exist.");

            targetFileSystem = HadoopFileSystemOps
                    .getFileSystemInstance(taskInfo.getTask().getTargetCatalog(), taskInfo.getTask().getTarget());

            System.out.println("Replication started for delta "+ taskInfo.getTask().getDelta());
            int count = 0;
            boolean isSucceeded = false;
            targetFilePath = new Path(taskInfo.getTask().getTarget() + taskInfo.getTask().getDelta());

            if (targetFileSystem.exists(targetFilePath))
                targetFileSystem.delete(targetFilePath, true);

            while (count < retry && !isSucceeded) {
                try {
                    HadoopFileOps.copy(sourceFileSystem,
                            sourceFilePath,
                            targetFileSystem,
                            targetFilePath,
                            blockSize);
                    isSucceeded = true;
                    this.status = TaskStatus.COMPLETED;
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                    this.status = TaskStatus.FAILED;
                }
                count++;
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            this.status = TaskStatus.FAILED;
        }

        // update task status to master.
        try{
            if (this.status != TaskStatus.COMPLETED) {
                try {
                    if (targetFileSystem != null && targetFilePath != null)
                        targetFileSystem.delete(targetFilePath, true);
                } catch (Exception ex) {System.out.println(ex.getMessage());}
                t1 = TaskInfo
                        .newBuilder(taskInfo)
                        .setTask(Task.newBuilder(taskInfo.getTask()).setStatus(TaskStatus.FAILED).build())
                        .build();
                System.out.println("Replication failed for task: "+t1);
            } else {
                t1 = TaskInfo
                        .newBuilder(taskInfo)
                        .setTask(Task.newBuilder(taskInfo.getTask()).setStatus(TaskStatus.COMPLETED).build())
                        .build();
                System.out.println("Replication completed for task: "+ t1);
            }
            this.masterClient.taskStatusUpdate(t1);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HdfsFileReplicationWorker that = (HdfsFileReplicationWorker) o;
        return taskInfo.getTask().getTaskId().equals(that.taskInfo.getTask().getTaskId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskInfo.getTask().getTaskId());
    }
}
