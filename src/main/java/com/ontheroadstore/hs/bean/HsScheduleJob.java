package com.ontheroadstore.hs.bean;

import java.io.Serializable;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/15.
 */
public class HsScheduleJob implements Serializable {
    private int id;
    private int type;
    private String biz_table_name;
    private String condition_field_name;
    private String condition_field_value;
    private int condition_field_type;
    private String be_updated_field_name;
    private int field_original_value;
    private int field_final_value;
    private int timing_cycle;
    private int timing_unit;
    private String create_time;
    private int status;
    private String attachment_script;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getBiz_table_name() {
        return biz_table_name;
    }

    public void setBiz_table_name(String biz_table_name) {
        this.biz_table_name = biz_table_name;
    }

    public String getCondition_field_name() {
        return condition_field_name;
    }

    public void setCondition_field_name(String condition_field_name) {
        this.condition_field_name = condition_field_name;
    }

    public String getCondition_field_value() {
        return condition_field_value;
    }

    public void setCondition_field_value(String condition_field_value) {
        this.condition_field_value = condition_field_value;
    }

    public int getCondition_field_type() {
        return condition_field_type;
    }

    public void setCondition_field_type(int condition_field_type) {
        this.condition_field_type = condition_field_type;
    }

    public String getBe_updated_field_name() {
        return be_updated_field_name;
    }

    public void setBe_updated_field_name(String be_updated_field_name) {
        this.be_updated_field_name = be_updated_field_name;
    }

    public int getField_original_value() {
        return field_original_value;
    }

    public void setField_original_value(int field_original_value) {
        this.field_original_value = field_original_value;
    }

    public int getField_final_value() {
        return field_final_value;
    }

    public void setField_final_value(int field_final_value) {
        this.field_final_value = field_final_value;
    }

    public int getTiming_cycle() {
        return timing_cycle;
    }

    public void setTiming_cycle(int timing_cycle) {
        this.timing_cycle = timing_cycle;
    }

    public int getTiming_unit() {
        return timing_unit;
    }

    public void setTiming_unit(int timing_unit) {
        this.timing_unit = timing_unit;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getAttachment_script() {
        return attachment_script;
    }

    public void setAttachment_script(String attachment_script) {
        this.attachment_script = attachment_script;
    }
}
