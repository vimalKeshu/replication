package org.vimalkeshu.replication.master;

import lombok.Getter;
import lombok.Setter;
import org.vimalkeshu.replication.grpc.messages.Job;
import org.vimalkeshu.replication.grpc.messages.JobIdInfo;
import org.vimalkeshu.replication.grpc.messages.JobStatus;
import org.vimalkeshu.replication.grpc.messages.JobStatusInfo;

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
            for (Map.Entry<String, Job> jobEntry: this.manager.getJobTracker().entrySet()) {
                try {
                    String jobId = jobEntry.getKey();
                    Job job = jobEntry.getValue();
                    if (job.getJobStatus() == JobStatus.running) {
                        JobStatusInfo jobStatusInfo = this.manager
                                                        .updateJobStatus(
                                                                JobIdInfo
                                                                        .newBuilder()
                                                                        .setJobId(jobId)
                                                                        .build());
                        System.out.println("Status: jobId: "+ jobStatusInfo.getJobId() +" - "+ jobEntry.getValue().getJobStatus()) ;
                    } else System.out.println("Status: jobId: "+ jobId +" - "+ jobEntry.getValue().getJobStatus()) ;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                if (stop) break;
            }
            try {Thread.sleep(5000);} catch (Exception ignored){}
        }
    }
}
