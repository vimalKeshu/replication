package org.vimalkeshu.replication.master;

import lombok.Getter;
import lombok.Setter;
import org.vimalkeshu.replication.common.grpc.messages.Job;
import org.vimalkeshu.replication.common.grpc.messages.JobIdInfo;
import org.vimalkeshu.replication.common.grpc.messages.JobStatusInfo;
import org.vimalkeshu.replication.common.grpc.messages.TaskInfo;
import org.vimalkeshu.replication.common.grpc.services.WorkerGrpc;

import java.util.Map;

@Getter
public class JobStatusUpdaterWorker implements Runnable {
    private final MasterManager manager;
    @Setter
    private volatile boolean stop;

    public JobStatusUpdaterWorker(MasterManager manager) {
        this.manager = manager;
        this.stop = false;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                for (Map.Entry<String, Job> jobEntry: this.manager.getJobTracker().entrySet()) {
                    System.out.println(this.manager
                            .updateJobStatus(
                                    JobIdInfo
                                            .newBuilder()
                                            .setJobId(jobEntry.getKey())
                                            .build()));
                    if (stop) break;
                }
                try {Thread.sleep(5000);} catch (Exception ignored){}
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
    }
}
