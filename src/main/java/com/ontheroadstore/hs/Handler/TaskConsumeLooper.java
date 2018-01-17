package com.ontheroadstore.hs.Handler;

import com.ontheroadstore.hs.App;
import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/16.
 */
public class TaskConsumeLooper extends DbLooper {

    private final Logger logger = Logger.getLogger(TaskConsumeLooper.class);
    private final int sleepTime = 2000; //2 seconds
    private final int poolSize = 20;
    private ScheduledExecutorService executorService;
    public TaskConsumeLooper(App app) {
        super(app);
        executorService = Executors.newScheduledThreadPool(poolSize);
        logger.debug("ExecutorService started with pool size:" + poolSize);
    }

    int doBusiness() {
        HsScheduleJob job = null;
        job = getApp().getLocalCacheHandler().poll();
        if (job == null) return sleepTime;
        try {
            executorService.schedule(new WorkerThread(job,this),job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
            logger.info("Job(ID:" + job.getId() + ") in working.");
            return 0;
        } catch (RejectedExecutionException e) {
            logger.error("Rejected job(ID:"+ job.getId() + ") back to queue," + e.getMessage());
            getApp().getLocalCacheHandler().add(job);
            return sleepTime;
        }
    }
    private TimeUnit getTimeUint(int timingUint) {
        if(timingUint==0) return TimeUnit.MINUTES;
        if(timingUint==1) return TimeUnit.HOURS;
        if(timingUint==2) return TimeUnit.DAYS;
        return TimeUnit.MINUTES;
    }

}
