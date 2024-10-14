package org.vimalkeshu.replication.master;

import lombok.Setter;
import org.vimalkeshu.replication.common.grpc.messages.Job;
import org.vimalkeshu.replication.common.grpc.messages.TaskInfo;
import org.vimalkeshu.replication.common.grpc.services.WorkerGrpc;

import java.util.Map;

public class JobStatusUpdaterWorker implements Runnable {
    private final MasterManager manager;
    private final Job job;
    @Setter
    private volatile boolean stop;

    public JobStatusUpdaterWorker(MasterManager manager, Job job) {
        this.manager = manager;
        this.job = job;
        this.stop = false;
    }

    @Override
    public void run() {
        while (!stop) {
            if (!this.manager.getWorkers().isEmpty()) {
                try {
                    for (Map.Entry<String, WorkerGrpc.WorkerBlockingStub> worker: manager.getWorkers().entrySet()) {
                        if (!manager.getTaskInfoQueue().isEmpty()) {
                            TaskInfo taskInfo = manager.getTaskInfoQueue().poll();
                            worker.getValue().taskAssignment(taskInfo);
                            System.out.println("Task: "+taskInfo +", assigned to worker: "+ worker.getKey());
                        }
                        if (stop) break;
                    }
                } catch (Exception ex){
                    System.out.println(ex.getMessage());
                }
            }
        }
    }
}
