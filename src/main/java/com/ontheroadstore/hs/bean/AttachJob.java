package com.ontheroadstore.hs.bean;

import java.io.Serializable;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/3/8.
 */
public class AttachJob implements Serializable{
    private int type;
    private String desc;
    private HsScheduleJob job;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public HsScheduleJob getJob() {
        return job;
    }

    public void setJob(HsScheduleJob job) {
        this.job = job;
    }
}
