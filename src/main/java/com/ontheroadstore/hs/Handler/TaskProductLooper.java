package com.ontheroadstore.hs.Handler;
import com.ontheroadstore.hs.App;
import com.ontheroadstore.hs.AppConstent;
import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.log4j.Logger;
import org.codejargon.fluentjdbc.api.FluentJdbcException;
import org.codejargon.fluentjdbc.api.FluentJdbcSqlException;
import org.codejargon.fluentjdbc.api.mapper.Mappers;
import org.codejargon.fluentjdbc.api.query.Mapper;
import org.codejargon.fluentjdbc.api.query.Query;
import org.codejargon.fluentjdbc.api.query.UpdateResult;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/15.
 */
public class TaskProductLooper extends DbLooper {

    private final Logger logger = Logger.getLogger(TaskProductLooper.class);
    private final int sleepTime = 60000; //60 seconds
    private final int pageSize = 1000;
    public TaskProductLooper(App app) {
        super(app);
        loadRunningJob();
    }

    int doBusiness() {
        if (getFluentJdbc()==null) {
            buildFluentJdbc();
        }
        int count = this.getTaskCounts(0);
        if (count <= 0) {
            return sleepTime;
        }
        int totalPages = (int)Math.floor(count/pageSize) + 1;
        for (int page=1;page<=totalPages;page++) {
            List<HsScheduleJob> jobs = getOnePageJobs(0,page,pageSize);
            if (jobs == null) continue;
            if (jobs.isEmpty()) continue;
            if(getApp().getLocalCacheHandler().addAll(jobs)){
                updateJobsStatus(jobs);
            }
        }
        logger.info("Done total pages:" + totalPages);
        return 1000;
    }
    private void loadRunningJob() {
        if (getFluentJdbc()==null) {
            buildFluentJdbc();
        }

        int count = this.getTaskCounts(1);
        logger.info("Loading running jobs,total:" + count);
        int totalPages = (int)Math.floor(count/pageSize) + 1;
        for (int page=1;page<=totalPages;page++) {
            List<HsScheduleJob> jobs = getOnePageJobs(1,page,pageSize);
            if (jobs == null) continue;
            if (jobs.isEmpty()) continue;
            getApp().getLocalCacheHandler().addAll(jobs);
        }
    }
    private boolean updateJobsStatus(List<HsScheduleJob> jobs) {
        Query query = getFluentJdbc().query();

        StringBuffer sbIds = new StringBuffer();
        for (HsScheduleJob job:
             jobs) {
            sbIds.append(job.getId()).append(',');
        }
        sbIds.deleteCharAt(sbIds.length()-1);

        DateTime dateTime = DateTime.now();
        String dateSTime = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
        String updateSql = "UPDATE sp_hs_schedule_jobs SET status = "
                + AppConstent.JOB_STATUS_DOING
                + ",update_time=? WHERE status = 0 AND id in (" + sbIds + ")";
        try {
            UpdateResult result = query.update(updateSql)
                    .params(dateSTime)
                    .run();
            if (result.affectedRows()>0) {
                return true;
            } else {
                logger.error("update status failed." + sbIds.toString());
            }
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause());
        }
        return false;
    }
    private int getTaskCounts(int status) {
        if (getFluentJdbc() == null) {
            return 0;
        }
        Query query = getFluentJdbc().query();
        String countSql = "SELECT count(*) FROM sp_hs_schedule_jobs WHERE status = " + status;
        int count = 0;
        try {
            count = query.select(countSql)
                    .singleResult(Mappers.singleInteger());

        } catch (FluentJdbcSqlException e) {
            logger.error( e.getMessage() + ",cause:" + e.getCause());
        }
        return count;
    }

    private List<HsScheduleJob> getOnePageJobs(int status,int page,int pageSize) {
        if (getFluentJdbc() == null) {
            return null;
        }
        Query query = getFluentJdbc().query();
        if(query == null) {
            return null;
        }
        int start = (page - 1) * pageSize;
        int end = pageSize;
        String querySql = "SELECT * FROM sp_hs_schedule_jobs WHERE status = " + status + " limit " + start + "," + end;
        List<HsScheduleJob> result = null;
        try {
            result = query.select(querySql)
                    .listResult(new Mapper<HsScheduleJob>() {
                        public HsScheduleJob map(ResultSet rs) throws SQLException {
                            HsScheduleJob hsScheduleJob = new HsScheduleJob();
                            hsScheduleJob.setId(rs.getInt("id"));
                            hsScheduleJob.setType(rs.getInt("type"));
                            hsScheduleJob.setAttachment_script(rs.getString("attachment_script"));
                            hsScheduleJob.setBe_updated_field_name(rs.getString("be_updated_field_name"));
                            hsScheduleJob.setBiz_table_name(rs.getString("biz_table_name"));
                            hsScheduleJob.setCondition_field_name(rs.getString("condition_field_name"));
                            hsScheduleJob.setCondition_field_type(rs.getInt("condition_field_type"));
                            hsScheduleJob.setCondition_field_value(rs.getString("condition_field_value"));
                            hsScheduleJob.setCreate_time(rs.getString("create_time"));
                            hsScheduleJob.setStatus(rs.getInt("status"));
                            hsScheduleJob.setTiming_cycle(rs.getInt("timing_cycle"));
                            hsScheduleJob.setTiming_unit(rs.getInt("timing_unit"));
                            hsScheduleJob.setField_final_value(rs.getInt("field_final_value"));
                            hsScheduleJob.setField_original_value(rs.getInt("field_original_value"));
                            hsScheduleJob.setOriginal_sql(rs.getString("original_sql"));
                            hsScheduleJob.setUpdate_time(rs.getString("update_time"));
                            return hsScheduleJob;
                        }
                    });
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage() + ",cause:" + e.getCause());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return  result;
    }

}
