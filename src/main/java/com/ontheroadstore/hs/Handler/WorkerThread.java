package com.ontheroadstore.hs.Handler;

import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.log4j.Logger;
import org.codejargon.fluentjdbc.api.query.Query;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/17.
 */
public class WorkerThread implements Runnable {

    private HsScheduleJob job;
    private DbLooper looper;
    private final Logger logger = Logger.getLogger(WorkerThread.class);
    public WorkerThread(HsScheduleJob job, DbLooper looper) {
        this.job = job;
        this.looper = looper;
    }

    @Override
    public void run() {
        logger.error("Hi i am worker. i am do job:"  + job.getId());

        Query query = this.looper.getFluentJdbc().query();

    }
}
