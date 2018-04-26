package com.ontheroadstore.hs.Handler;

import com.google.gson.Gson;
import com.mysql.jdbc.StringUtils;
import com.ontheroadstore.hs.App;
import com.ontheroadstore.hs.AppConstent;
import com.ontheroadstore.hs.bean.AttachJob;
import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.log4j.Logger;
import org.codejargon.fluentjdbc.api.FluentJdbcException;
import org.codejargon.fluentjdbc.api.mapper.Mappers;
import org.codejargon.fluentjdbc.api.query.Mapper;
import org.codejargon.fluentjdbc.api.query.Query;
import org.codejargon.fluentjdbc.api.query.UpdateResult;
import org.codejargon.fluentjdbc.api.query.UpdateResultGenKeys;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/17.
 */
public class WorkerThread implements Runnable {

    private App app;
    private HsScheduleJob job;
    private DbLooper looper;
    private final Logger logger = Logger.getLogger(WorkerThread.class);
    public WorkerThread(HsScheduleJob job, DbLooper looper) {
        this.job = job;
        this.looper = looper;
    }

    public WorkerThread(HsScheduleJob job, DbLooper looper,App app) {
        this(job,looper);
        this.app = app;
    }

    @Override
    public void run() {
        logger.info("To do job(ID:" + job.getId() + ").");
        if (looper.getFluentJdbc()==null) {
            logger.error("Jdbc connection is null.can't finish job(ID:" + job.getId()+ ")");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

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
            logger.error("Job(ID:" + job.getId() + ") Failed.");
            updateJobStatus(job.getId(),AppConstent.JOB_STATUS_DONE_RESULT_FAILED);
            return;
        }
        logger.info("Job(ID:" + job.getId() + ") done.");
        updateJobStatus(job.getId(),AppConstent.JOB_STATUS_DONE);
        if(job.getType()==AppConstent.JOB_TYPE_EXECUTE_SCRIPT) {
            return;
        }
        if (job.getBiz_table_name().equals(AppConstent.REFUND_TABLE_NAME)){
            int evidenceId = 0;
            if(job.getType() == AppConstent.JOB_TYPE_NORMAL) {
                evidenceId = (int)insertRefundEvidence(job);
            }
            result = refundOperateLog(job,evidenceId);
            if (!result) {
                logger.error("Record refund log error!");
            }
        }

        //检查status == task_id 的附加任务，有执行他的script

        doTriggerScript(job.getId());

    }

    private boolean doAttachmentJob(HsScheduleJob job) {
        if (StringUtils.isNullOrEmpty(job.getAttachment_script())) return false;
        logger.debug("New job start:" + job.getAttachment_script());
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
    private boolean doNewTask(HsScheduleJob job) {
        try {
            getLooper().getApp().getLocalCacheHandler().add(job);
            return true;
        }catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }
    private UpdateResult updateJobStatus(int jobId,int status) {
        if (jobId == 0) {
            return null;
        }
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
    private long insertRefundEvidence(HsScheduleJob job) {
        if (!job.getBiz_table_name().equals("sp_hs_new_refund")) {
            return 0L;
        }
        if(StringUtils.isNullOrEmpty(job.getOriginal_sql())) return 0L;
        List<Object> params = new ArrayList();
        //reufnd_id,operator,operate_uid,reason,attachment,created_at,updated_at;
        //refund_id,4,0,
        params.add(job.getCondition_field_value());
        params.add(4);
        params.add(0);
        params.add("系统自动同意");
        params.add(job.getOriginal_sql());
        DateTime dateTime = DateTime.now();
        String dateSTime = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
        params.add(dateSTime);
        params.add(dateSTime);
        String insertSql = "INSERT INTO sp_hs_refund_evidence(refund_id,operator,operator_uid,reason,attachment,created_at,updated_at) VALUES(?,?,?,?,?,?,?)";
        try {
            UpdateResultGenKeys<Long> result = this.looper.getFluentJdbc()
                    .query()
                    .update(insertSql)
                    .params(params)
                    .runFetchGenKeys(Mappers.singleLong());
            return result.generatedKeys().get(0);
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause() + " params:" + new Gson().toJson(params));
            return 0L;
        }
    }

    private void doTriggerScript(int taskId) {
        logger.debug("Do trigger script for task id: " + taskId);
        List<HsScheduleJob> jobs = getScriptJobs(taskId);
        if(jobs==null) {
            logger.debug("Not found attachment script return null.");
            return;
        }
        if(jobs.isEmpty()) {
            logger.debug("Not found attachment script.");
            return;
        }
        app.getLocalCacheHandler().addAll(jobs);
    }

    private List<HsScheduleJob> getScriptJobs(int status) {
        if (this.looper.getFluentJdbc() == null) {
            logger.error("Can not load jdbc handler.");
            return null;
        }
        Query query = this.looper.getFluentJdbc().query();
        if(query == null) {
            logger.error("query object is null.");
            return null;
        }

        String querySql = "SELECT * FROM sp_hs_schedule_jobs WHERE status = " + status ;
        List<HsScheduleJob> result = null;
        try {

            result = query.select(querySql)
                    .listResult(new Mapper<HsScheduleJob>() {

                        public HsScheduleJob map(ResultSet rs) throws SQLException {

                            HsScheduleJob job = new HsScheduleJob();
                            job.setId(rs.getInt("id"));
                            job.setType(rs.getInt("type"));
                            job.setAttachment_script(rs.getString("attachment_script"));
                            job.setBe_updated_field_name(rs.getString("be_updated_field_name"));
                            job.setBiz_table_name(rs.getString("biz_table_name"));
                            job.setCondition_field_name(rs.getString("condition_field_name"));
                            job.setCondition_field_type(rs.getInt("condition_field_type"));
                            job.setCondition_field_value(rs.getString("condition_field_value"));
                            job.setCreate_time(rs.getString("create_time"));
                            job.setStatus(rs.getInt("status"));
                            job.setTiming_cycle(rs.getInt("timing_cycle"));
                            job.setTiming_unit(rs.getInt("timing_unit"));
                            job.setField_final_value(rs.getInt("field_final_value"));
                            job.setField_original_value(rs.getInt("field_original_value"));
                            job.setOriginal_sql("");
                            job.setUpdate_time(rs.getString("update_time"));
                            return job;
                        }
                    });
        } catch (FluentJdbcException e) {

            logger.error(e.getMessage() + ",cause:" + e.getCause());

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return  result;
    }

    private boolean refundOperateLog(HsScheduleJob job,int evidenceId) {
        if (!job.getBiz_table_name().equals("sp_hs_new_refund")) {
            return false;
        }
        List<Object> params = new ArrayList();
        //reufnd_id,evidence_id,operator,operate_uid,old_status,final_status,created_at,updated_at;
        //refund_id,0,4,0,
        params.add(job.getCondition_field_value());
        params.add(evidenceId);
        params.add(4);
        params.add(0);
        params.add(job.getField_original_value());
        params.add(job.getField_final_value());
        DateTime dateTime = DateTime.now();
        String dateSTime = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
        params.add(dateSTime);
        params.add(dateSTime);
        String logSql = "INSERT INTO sp_hs_refund_operate_log(refund_id,evidence_id,operator,operate_uid,old_status,final_status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)";
        try {
            UpdateResult result = this.looper.getFluentJdbc()
                    .query()
                    .update(logSql)
                    .params(params)
                    .run();
            if (result.affectedRows()<=0) return false;
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause() + " params:" + new Gson().toJson(params));
            return false;
        }
        return true;
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

        if(StringUtils.isNullOrEmpty(job.getAttachment_script())) {
            logger.error("Script job. script is empty.");
            return false;
        }
        if(StringUtils.isNullOrEmpty(app.getRedisMessageQueueKey())) {
            logger.error("Redis queue key not found.");
            return false;
        }
        Jedis jedis = app.getJedisPoolHandler().getResource();
        if(jedis == null) {
            return false;
        }
        try {
            long result = jedis.rpush(app.getRedisMessageQueueKey(),job.getAttachment_script());
            jedis.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
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

        if (job.getBiz_table_name().equals(AppConstent.ORDER_TABLE_NAME) ) {
            return doOrderJob(job);
        } else if(job.getBiz_table_name().equals(AppConstent.REFUND_TABLE_NAME)) {
            return doRefundJob(job);
        }
        return true;
    }
    private boolean doRefundJob(HsScheduleJob job) {
        String updateSql = "UPDATE " + job.getBiz_table_name()
                + " SET "
                + job.getBe_updated_field_name()
                + " = :update_value"
                + ",updated_at = :update_at"
                + " WHERE "
                + job.getCondition_field_name()
                + " = :condition_value "
                + "AND "
                + job.getBe_updated_field_name()
                + " = :old_value";
        Map<String,Object> namedParams = new HashMap<>();
        namedParams.put("condition_value",job.getCondition_field_value());
        namedParams.put("old_value",job.getField_original_value());
        namedParams.put("update_value",job.getField_final_value());
        namedParams.put("updated_at",DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")));
        if(!doUpdateSql(updateSql,namedParams)) return false;
        doAttachmentJob(job);
        return true;
    }

    private boolean doOrderJob(HsScheduleJob job) {
        boolean isUnpaid = false;

        if(job.getField_original_value()==0) {
            isUnpaid = true;
        }

        String conditionFieldName = job.getCondition_field_name();
        if(job.getCondition_field_value().startsWith("VR")) {
            conditionFieldName = "union_order_number";
        }

        String updateTimeFieldName = isUnpaid?"complete_time":"deliver_time";
        String updateSql = "UPDATE " + job.getBiz_table_name()
                + " SET "
                + job.getBe_updated_field_name()
                + " = :update_value"
                + "," + updateTimeFieldName
                + " = :time_at"
                + " WHERE "
                + conditionFieldName
                + " = :condition_value "
                + "AND "
                + job.getBe_updated_field_name()
                + " = :old_value";
                if (isUnpaid) {
                    updateSql += " AND order_status=0";
                }
        Map<String,Object> namedParams = new HashMap<>();
        namedParams.put("condition_value",job.getCondition_field_value());
        namedParams.put("old_value",job.getField_original_value());
        namedParams.put("update_value",isUnpaid?AppConstent.ORDER_PROCESS_STATUS_CLOSED:job.getField_final_value());
        namedParams.put("time_at",DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")));
        logger.debug("SQL params:" + new Gson().toJson(namedParams));
        if(!doUpdateSql(updateSql,namedParams)) return false;
        doAttachmentJob(job);
        return true;
    }


    private boolean doUpdateSql(String updateSql,Map<String,Object> namedParams) {
        Query query = looper.getFluentJdbc().query();
        try {
            UpdateResult rs = query.update(updateSql)
                    .namedParams(namedParams)
                    .run();

            if(rs.affectedRows()<=0) {
                logger.error("No affect result job(ID:" + job.getId() + ")");
                return false;
            }
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause());
            return false;
        }
        return true;
    }

    public DbLooper getLooper() {
        return looper;
    }
}
