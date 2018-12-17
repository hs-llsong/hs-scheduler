package com.ontheroadstore.hs;

import com.ontheroadstore.hs.Handler.*;
import com.ontheroadstore.hs.bean.HsScheduleJob;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/15.
 */
public class App {
    private final static Logger logger = Logger.getLogger(App.class);
    private Properties prop;
    private DataSource ds;
    private LocalCacheHandler<HsScheduleJob> localCacheHandler;
    private JedisPoolHandler jedisPoolHandler;
    private Map<String,DbLooper> looperMap = new HashMap<>();
    private String redisMessageQueueKey;
    public static void main( String[] args ){
        App app = new App();
        Properties prop = app.loadProperties();
        if (prop == null) {
            System.out.println("Can't load properties");
            System.exit(-1);
        }
        app.setProp(prop);

        String redis_host,redis_auth,redis_sport,redis_queue_key;
        int redis_port = 6379;
        redis_host = prop.getProperty(AppPropertiesKey.REDIS_HOST_KEY);
        redis_auth = prop.getProperty(AppPropertiesKey.REDIS_AUTH_KEY);
        redis_sport = prop.getProperty(AppPropertiesKey.REDIS_PORT_KEY);
        redis_queue_key = prop.getProperty(AppPropertiesKey.REDIS_MESSAGE_PUSH_CACHE_KEY);
        app.setRedisMessageQueueKey(redis_queue_key);
        if (StringUtils.isEmpty(redis_host)) {
            System.out.println("Redis host must be set.");
            System.exit(0);
        }
        if (!StringUtils.isEmpty(redis_sport)) {
            redis_port = Integer.valueOf(redis_sport);
        }
        app.setJedisPoolHandler(new JedisPoolHandler(redis_host,redis_port,redis_auth,3000));

        app.setUpDataSource();
        app.setLocalCacheHandler(new LocalCacheHandler<>());
        TaskProductLooper productLooper = new TaskProductLooper(app);
        TaskConsumeLooper consumeLooper = new TaskConsumeLooper(app);
        app.getLooperMap().put("product",productLooper);
        app.getLooperMap().put("consume",consumeLooper);
        System.out.println("To start product thread.");
        Thread t1 = new Thread(productLooper);
        t1.start();
        System.out.println("To start consume thread.");
        Thread t2 = new Thread(consumeLooper);
        t2.start();
        System.out.println("OK.");
        Runtime.getRuntime().addShutdownHook(new ShutDownHook(app));
    }

    public Map<String, DbLooper> getLooperMap() {
        return looperMap;
    }

    public void setLooperMap(Map<String, DbLooper> looperMap) {
        this.looperMap = looperMap;
    }

    public LocalCacheHandler<HsScheduleJob> getLocalCacheHandler() {
        return localCacheHandler;
    }

    public void setLocalCacheHandler(LocalCacheHandler<HsScheduleJob> localCacheHandler) {
        this.localCacheHandler = localCacheHandler;
    }

    public Properties getProp() {
        return prop;
    }

    public void setProp(Properties prop) {
        this.prop = prop;
    }

    public Properties loadProperties() {
        Properties prop = new Properties();
        try {
            prop.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        } catch (NullPointerException e) {
            logger.error(e.getMessage());
            return null;
        }
        return prop;
    }

    public DataSource setUpDataSource() {
        if(this.ds!=null) {
            try {
                this.ds.getConnection().close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
                return null;
            }
        }
        String host = prop.getProperty(AppPropertiesKey.DB_HOST_KEY);
        String db_name = prop.getProperty(AppPropertiesKey.DB_NAME_KEY);
        if (host == null) {
            return null;
        }
        if (db_name == null || db_name.equals("")) {
            return null;
        }
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://" +
                prop.getProperty(AppPropertiesKey.DB_HOST_KEY) + ":3306/" +
                prop.getProperty(AppPropertiesKey.DB_NAME_KEY) + "?autoReconnect=true";
        ds.setUrl(url);
        ds.setUsername(prop.getProperty(AppPropertiesKey.DB_USER_KEY));
        ds.setPassword(prop.getProperty(AppPropertiesKey.DB_PASSWORD_KEY));
        this.ds = ds;
        return ds;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    static class ShutDownHook extends Thread {
        App app;
        public ShutDownHook(App app) {
            this.app = app;
        }

        @Override
        public void run() {
            for (DbLooper looper:app.getLooperMap().values()) {
                looper.stop();
            }
        }
    }

    public JedisPoolHandler getJedisPoolHandler() {
        return jedisPoolHandler;
    }

    public void setJedisPoolHandler(JedisPoolHandler jedisPoolHandler) {
        this.jedisPoolHandler = jedisPoolHandler;
    }

    public String getRedisMessageQueueKey() {
        return redisMessageQueueKey;
    }

    public void setRedisMessageQueueKey(String redisMessageQueueKey) {
        this.redisMessageQueueKey = redisMessageQueueKey;
    }
}
