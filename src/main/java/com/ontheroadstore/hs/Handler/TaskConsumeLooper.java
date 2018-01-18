package com.ontheroadstore.hs.Handler;

import com.ontheroadstore.hs.App;
import com.ontheroadstore.hs.AppConstent;
import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
        if (getFluentJdbc()==null) {
            buildFluentJdbc();
        }
        HsScheduleJob job = null;
        job = getApp().getLocalCacheHandler().poll();
        if (job == null) return sleepTime;
        try {
            if (job.getStatus()== AppConstent.JOB_STATUS_TODO)
                executorService.schedule(new WorkerThread(job,this),job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
            else {
                //DOING job ,continue
                DateTime nowDateTime = DateTime.now();
                DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                DateTime updateDateTime = DateTime.parse(job.getUpdate_time(),format);
                int escapedMinutes = Minutes.minutesBetween(updateDateTime,nowDateTime).getMinutes();
                int totalMinutes = getTotalMinutes(job);
                int remain = totalMinutes - escapedMinutes;
                if(remain<0) remain = 0;
                logger.info("Continue to do job(ID:" + job.getId() + ") with remain minutes:" + remain);
                executorService.schedule(new WorkerThread(job,this),remain,TimeUnit.MINUTES);

            }
            logger.info("Job(ID:" + job.getId() + ") in working schedule.");
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

    private int getTotalMinutes(HsScheduleJob job) {
        if(job.getTiming_unit()==1) return job.getTiming_cycle()*60;
        if(job.getTiming_unit()==2) return job.getTiming_cycle()*24*60;
        return job.getTiming_cycle();
    }
}
