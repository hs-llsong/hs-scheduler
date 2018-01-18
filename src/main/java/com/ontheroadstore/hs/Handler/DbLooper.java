package com.ontheroadstore.hs.Handler;

import com.ontheroadstore.hs.App;
import org.apache.log4j.Logger;
import org.codejargon.fluentjdbc.api.FluentJdbc;
import org.codejargon.fluentjdbc.api.FluentJdbcBuilder;
import org.codejargon.fluentjdbc.api.FluentJdbcException;

import javax.sql.DataSource;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/16.
 */
public abstract class DbLooper implements Runnable {
    private App app;
    private FluentJdbc fluentJdbc;
    private boolean stop = false;
    private final Logger logger = Logger.getLogger(DbLooper.class);
    public DbLooper(App app) {
        this.app = app;
    }

    public void run() {
        while (!stop) {
            int result = doBusiness();
            if (result>0) {
                this.sleep(result);
                continue;
            }
            Thread.yield();
        }
        System.out.println("Quit looper");
    }

    abstract int doBusiness();

    public FluentJdbc getFluentJdbc() {
        return fluentJdbc;
    }
    protected FluentJdbc buildFluentJdbc() {
        DataSource ds = app.getDs();
        if (ds == null) {
            System.out.println("DataSource config error.");
            return null;
        }
        try {
            FluentJdbcBuilder fluentJdbcBuilder = new FluentJdbcBuilder();
            fluentJdbcBuilder.connectionProvider(app.getDs());
            fluentJdbcBuilder.afterQueryListener(executionDetails -> {
                if (!executionDetails.success()) {
                    logger.error(executionDetails.sql());
                }
            });

            this.fluentJdbc = fluentJdbcBuilder.build();
        } catch (FluentJdbcException e) {
            logger.error(e.getMessage());
            return null;
        }

        return this.fluentJdbc;
    }

    public void stop(){
        this.stop = true;
    }

    public App getApp() {
        return app;
    }

    void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.yield();
        }
    }
}
