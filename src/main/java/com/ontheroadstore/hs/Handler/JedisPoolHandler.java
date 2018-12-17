package com.ontheroadstore.hs.Handler;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 17/11/24.
 */
public class JedisPoolHandler {
    private JedisPool jedisPool;
    private String host;
    private int port;
    private String auth;
    private int timeout;
    private final Logger logger = Logger.getLogger(JedisPoolHandler.class);

    public JedisPoolHandler(String host, int port, String auth, int timeout) {
        this.host = host;
        this.port = port;
        this.auth = auth;
        this.timeout = timeout;
        createPool();
    }

    private void createPool() {
        jedisPool = null;
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(200);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(100000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        jedisPool = new JedisPool(config,host,port,timeout,auth);
    }

    public Jedis getResource() {
        try {
            return jedisPool.getResource();
        } catch (Exception e) {
            logger.error("jedisPool error: " + e.getMessage());
            return null;
        }
    }
}
