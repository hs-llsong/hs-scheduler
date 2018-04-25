package com.ontheroadstore.hs.Handler;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com)
 *
 */
public class JedisPoolHandler {
    private JedisPool jedisPool;
    private String host;
    private int port;
    private String auth;
    private int timeout;
    private final int MAX_IDLE = 5;
    private final int MAX_TOTAL = 18;
    private final Logger logger = Logger.getLogger(JedisPoolHandler.class);
    private final Lock lock = new ReentrantLock();
    private Condition reuseCondition = null;
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
        config.setMaxIdle(MAX_IDLE);
        config.setMaxTotal(MAX_TOTAL);
        jedisPool = new JedisPool(config,host,port,timeout,auth);
    }

    public void resetPool() {
        reuseCondition = lock.newCondition();
        lock.lock();
        try {
            jedisPool.close();
            this.createPool();
        } catch (JedisException e) {
            logger.error(e.getMessage());
        }
        finally {
            reuseCondition.signal();
            lock.unlock();
        }
    }
    public void close() {
        try {
            jedisPool.close();
        } catch (JedisException e) {
            logger.error(e.getMessage());
        }
    }

    public Jedis getResource() {
        if(reuseCondition!=null) {
            try {
                reuseCondition.await();
            }catch (InterruptedException e) {
                logger.error(e.getMessage());
            }catch (Exception e) {
                logger.error(e.getMessage());
            }

        }
        try {
            Jedis jedis = jedisPool.getResource();
            return jedis;
        } catch (JedisConnectionException e) {
            logger.error(e.getMessage());
            return null;
        } catch (Exception e) {
            return null;
        }
    }

}
