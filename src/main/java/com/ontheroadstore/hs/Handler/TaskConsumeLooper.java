package com.ontheroadstore.hs.Handler;

import com.mysql.jdbc.StringUtils;
import com.ontheroadstore.hs.App;
import com.ontheroadstore.hs.AppConstent;
import com.ontheroadstore.hs.AppPropertiesKey;
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
    private final int defualtPoolSize = 20;
    private ScheduledExecutorService executorService;

    public TaskConsumeLooper(App app) {
        super(app);
        String sPoolSize = app.getProp().getProperty(AppPropertiesKey.THREAD_POOL_SIZE_KEY);
        int poolSize = defualtPoolSize;
        if(!StringUtils.isNullOrEmpty(sPoolSize)) {
            poolSize = Integer.valueOf(sPoolSize);
        }
        executorService = Executors.newScheduledThreadPool(poolSize);
        logger.info("ExecutorService started with pool size:" + poolSize);
    }

    int doBusiness() {
        if (getFluentJdbc() == null) {
            buildFluentJdbc();
        }
        HsScheduleJob job = getApp().getLocalCacheHandler().poll();
        if (job == null) return sleepTime;

        if (job.getStatus() == AppConstent.JOB_STATUS_TODO) {
            if (StringUtils.isNullOrEmpty(job.getCreate_time())) {
                return doExecutorServiceSchedule(job, job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
            }
            return doAdjustExecuteTime(job.getCreate_time(), job);
        } else if(job.getStatus() == AppConstent.JOB_STATUS_REJECTED) {

            logger.info("To do rejected job(ID:" + job.getId() + ")");
            if (StringUtils.isNullOrEmpty(job.getCreate_time())) {
                return doExecutorServiceSchedule(job, job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
            } else {
                return doAdjustExecuteTime(job.getCreate_time(), job);
            }
        } else if(job.getStatus()>40) {
            logger.info("To do trigger job: " + job.getId());
            return doExecutorServiceSchedule(job, job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
        } else {

            if (StringUtils.isNullOrEmpty(job.getUpdate_time())) {
                logger.info("Update time is null.");
                return doExecutorServiceSchedule(job, job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
            } else {
                return doAdjustExecuteTime(job.getUpdate_time(), job);
            }

        }
    }

    private int doAdjustExecuteTime(String beginTime, HsScheduleJob job) {
        DateTime nowDateTime = DateTime.now();
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
        DateTime updateDateTime;
        try {
            updateDateTime = DateTime.parse(beginTime, format);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return doExecutorServiceSchedule(job, job.getTiming_cycle(), getTimeUint(job.getTiming_unit()));
        }
        int escapedMinutes = Minutes.minutesBetween(updateDateTime, nowDateTime).getMinutes();
        int totalMinutes = getTotalMinutes(job);
        int remain = totalMinutes - escapedMinutes;
        if (remain < 0) remain = 0;

        return doExecutorServiceSchedule(job, remain, TimeUnit.MINUTES);
    }

    private int doExecutorServiceSchedule(HsScheduleJob job, int timing, TimeUnit timeUnit) {
        try {
            executorService.schedule(new WorkerThread(job, this,getApp()), timing, timeUnit);
            logger.info("Job(ID:" + job.getId() + ") in working schedule.");
            return 0;
        } catch (RejectedExecutionException e) {
            logger.error("Rejected job(ID:" + job.getId() + ") back to queue," + e.getMessage());
            job.setStatus(AppConstent.JOB_STATUS_REJECTED);
            getApp().getLocalCacheHandler().add(job);
            return sleepTime;
        }
    }

    private TimeUnit getTimeUint(int timingUint) {
        if (timingUint == 0) return TimeUnit.MINUTES;
        if (timingUint == 1) return TimeUnit.HOURS;
        if (timingUint == 2) return TimeUnit.DAYS;
        return TimeUnit.MINUTES;
    }

    private int getTotalMinutes(HsScheduleJob job) {
        if (job.getTiming_unit() == 1) return job.getTiming_cycle() * 60;
        if (job.getTiming_unit() == 2) return job.getTiming_cycle() * 24 * 60;
        return job.getTiming_cycle();
    }
}
