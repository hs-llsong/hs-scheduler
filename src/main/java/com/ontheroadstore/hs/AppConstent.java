package com.ontheroadstore.hs;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/16.
 */
public class AppConstent {
    public static final int JOB_STATUS_DOING = 1;
    public static final int JOB_STATUS_DONE = 2;
    public static final int JOB_STATUS_TODO = 0;
    public static final int JOB_STATUS_REJECTED = 4;
    public static final int JOB_STATUS_GIVEUP = 31;
    public static final int JOB_STATUS_DONE_RESULT_FAILED = 12;

    public static final int JOB_TYPE_EXECUTE_SQL = 2;
    public static final int JOB_TYPE_EXECUTE_SCRIPT = 1;
    public static final int JOB_TYPE_NORMAL = 0;
    public static final int JOB_TYPE_PUSH_MESSAGE = 4;

    public static final int ATTACH_JOB_TYPE_NEWTASK = 1;
    public static final int ATTACH_JOB_TYPE_PUSH_MESSAGE = 2;

    public static final String ORDER_TABLE_NAME = "sp_hs_orders";
    public static final String REFUND_TABLE_NAME = "sp_hs_new_refund";
    public static final String GUISHI_ORDER_TABLE_NAME = "sp_hs_ghost_orders";
    public static final int ORDER_PROCESS_STATUS_CLOSED = 33;
}
