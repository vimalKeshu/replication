package org.vimalkeshu.replication.common;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JobService {
    private final ExecutorService executor;

    public JobService(int parallelism){
        this.executor = Executors.newWorkStealingPool(parallelism);
    }

    public void submit(Runnable runnable) {
        this.executor.submit(runnable);
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return this.executor.submit(callable);
    }

    public void shutDown(){
        if (executor!=null) executor.shutdown();
    }
}
