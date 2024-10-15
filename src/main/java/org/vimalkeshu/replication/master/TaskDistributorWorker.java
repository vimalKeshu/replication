package org.vimalkeshu.replication.master;

import lombok.Getter;
import lombok.Setter;
import org.vimalkeshu.replication.common.grpc.messages.TaskInfo;
import org.vimalkeshu.replication.common.grpc.services.WorkerGrpc;

import java.util.Map;

@Getter
public class TaskDistributorWorker implements Runnable {
    private final MasterManager manager;
    @Setter
    private volatile boolean stop;
    private final int workerParallelism = 100;

    public TaskDistributorWorker(MasterManager manager) {
        this.manager = manager;
        this.stop = false;
    }

    @Override
    public void run() {
        while (!stop) {
            if (!this.manager.getWorkers().isEmpty()) {
                try {
                    for (Map.Entry<String, WorkerGrpc.WorkerBlockingStub> worker: manager.getWorkers().entrySet()) {
                        int cnt = 0;
                        while (!manager.getTaskInfoQueue().isEmpty() && cnt < workerParallelism) {
                            TaskInfo taskInfo = manager.getTaskInfoQueue().poll();
                            worker.getValue().taskAssignment(taskInfo);
                            System.out.println("Task: "+taskInfo +", assigned to worker: "+ worker.getKey());
                            cnt++;
                        }
                        if (stop) break;
                    }
                } catch (Exception ex){
                    System.out.println(ex.getMessage());
                }
            }
            try {Thread.sleep(1000);} catch (Exception ignored){}
        }
    }
}
