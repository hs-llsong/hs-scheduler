package com.ontheroadstore.hs.Handler;

import com.google.gson.Gson;
import com.mysql.jdbc.StringUtils;
import com.ontheroadstore.hs.AppConstent;
import com.ontheroadstore.hs.bean.AttachJob;
import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.log4j.Logger;
import org.codejargon.fluentjdbc.api.FluentJdbcException;
import org.codejargon.fluentjdbc.api.query.Query;
import org.codejargon.fluentjdbc.api.query.UpdateResult;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.HashMap;
import java.util.Map;

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
        logger.info("To do job(ID:" + job.getId() + ").");
        if (looper.getFluentJdbc()==null) {
            logger.error("Jdbc connection is null.can't finish job(ID:" + job.getId()+ ")");
            try {
                Thread.sleep(1000);
            }catch (InterruptedException e) {

            }
            job.setTiming_cycle(0);
            looper.getApp().getLocalCacheHandler().add(job);
            return;
        }
        if (!checkJobParams(job)) {
            updateJobStatus(job.getId(),AppConstent.JOB_STATUS_GIVEUP);
            logger.error("Params error in job(ID:" + job.getId() + ")");
            return;
        }

        boolean result = doJob(job);
        if (!result) {
            logger.error("do job failed.");
            updateJobStatus(job.getId(),AppConstent.JOB_STATUS_DONE_RESULT_FAILED);
            return;
        }
        logger.info("Job(ID:" + job.getId() + ") done.");
        updateJobStatus(job.getId(),AppConstent.JOB_STATUS_DONE);

    }

    private boolean doAttachmentJob(HsScheduleJob job) {
        if (StringUtils.isNullOrEmpty(job.getAttachment_script())) return false;

        AttachJob attachJob = null;
        try {
            attachJob = new Gson().fromJson(job.getAttachment_script(), AttachJob.class);
        }catch (Exception e) {
            logger.error(e.getMessage()+ e.getCause());
            return false;
        }
        if (attachJob == null) return false;
        switch (attachJob.getType()) {
            case AppConstent.ATTACH_JOB_TYPE_NEWTASK:
                return doNewTask(attachJob.getJob());
            case AppConstent.ATTACH_JOB_TYPE_PUSH_MESSAGE:
                break;
            default:
                break;
        }
        return true;
    }
    private boolean doNewTask(Object job) {
        try {
            HsScheduleJob newJob = (HsScheduleJob) job;
            if(newJob == null)return false;
            getLooper().getApp().getLocalCacheHandler().add(newJob);
            return true;
        }catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }
    private UpdateResult updateJobStatus(int jobId,int status) {
        String updateStatusSql = "UPDATE sp_hs_schedule_jobs SET status = "
                + status
                + ",update_time=? WHERE id = " + jobId ;
        DateTime dateTime = DateTime.now();
        String dateSTime = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));

        try {
            return this.looper.getFluentJdbc().query().update(updateStatusSql).params(dateSTime).run();
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause());
            return null;
        }

    }
    private boolean checkJobParams(HsScheduleJob job) {

        if(job.getType()==AppConstent.JOB_TYPE_NORMAL) {
            if (StringUtils.isNullOrEmpty(job.getBiz_table_name())) {
                logger.error("Bad job(ID:" + job.getId() + ") BizTableName is empty! ");
                return false;
            }
            if (StringUtils.isNullOrEmpty(job.getCondition_field_name())) {
                logger.error("Bad job(ID:" + job.getId() + ") Condition field name is empty! ");
                return false;
            }
            if (job.getCondition_field_type()==0 && StringUtils.isNullOrEmpty(job.getCondition_field_value())) {
                logger.error("Bad job(ID:" + job.getId() + ") Condition field value is empty! ");
                return false;
            }
            if (StringUtils.isNullOrEmpty(job.getBe_updated_field_name())) {
                logger.error("Bad job(ID:" + job.getId() + ") Be update field name is empty! ");
                return false;
            }
        }

        if(job.getType()==AppConstent.JOB_TYPE_EXECUTE_SQL) {
            if(StringUtils.isNullOrEmpty(job.getOriginal_sql())) {
                logger.error("Bad job(ID:" + job.getId() + ") Original sql is empty! ");
                return false;
            }
        }

        if(job.getType() == AppConstent.JOB_TYPE_EXECUTE_SCRIPT) {
            if(StringUtils.isNullOrEmpty(job.getAttachment_script())) {
                logger.error("Bad job(ID:" + job.getId() + ") Attachment script is empty! ");
                return false;
            }
        }

        return true;
    }
    private boolean doJob(HsScheduleJob job) {
        if(job.getType()==AppConstent.JOB_TYPE_NORMAL) return doNormallyJob(job);
        if(job.getType()==AppConstent.JOB_TYPE_EXECUTE_SCRIPT) return doScriptJob(job);
        if(job.getType()==AppConstent.JOB_TYPE_EXECUTE_SQL) return doSqlJob(job);
        return false;
    }
    private boolean doScriptJob(HsScheduleJob job) {
        logger.info("Do script job(ID:" + job.getId() + ")");
        return true;
    }

    private boolean doSqlJob(HsScheduleJob job) {
        Query query = looper.getFluentJdbc().query();
        try {
            UpdateResult rs = query.update(job.getOriginal_sql())
                    .run();
            if(rs.affectedRows()<=0) {
                logger.error("No affect result job(ID:" + job.getId() + ")");
            }
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause());
            return false;
        }
        return true;
    }

    private boolean doNormallyJob(HsScheduleJob job) {
        String updateSql = "UPDATE " + job.getBiz_table_name()
                + " SET "
                + job.getBe_updated_field_name()
                + " = :update_value "
                + "WHERE "
                + job.getCondition_field_name()
                + " = :condition_value "
                + "AND "
                + job.getBe_updated_field_name()
                + " = :old_value";
        Map<String,Object> namedParams = new HashMap<>();
        namedParams.put("condition_value",job.getCondition_field_value());
        namedParams.put("old_value",job.getField_original_value());
        namedParams.put("update_value",job.getField_final_value());
        Query query = looper.getFluentJdbc().query();
        try {
            UpdateResult rs = query.update(updateSql)
                    .namedParams(namedParams)
                    .run();
            if(rs.affectedRows()<=0) {
                logger.error("No affect result job(ID:" + job.getId() + ")");
            }
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause());
            return false;
        }
        doAttachmentJob(job);
        return true;
    }

    public DbLooper getLooper() {
        return looper;
    }
}
