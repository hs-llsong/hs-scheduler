package com.ontheroadstore.hs.Handler;

import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/1/16.
 */
public class LocalCacheHandler<T> {
    private Queue<T> queue = new LinkedList<T>();
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Logger logger = Logger.getLogger(LocalCacheHandler.class);
    public T poll() {
        lock.readLock().lock();
        try {
            return queue.poll();
        }catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }
    public boolean add(T job) {
        lock.writeLock().lock();
        try {
            return queue.add(job);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean addAll(List<T> jobs) {
        lock.writeLock().lock();
        try {
            return queue.addAll(jobs);
        }catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
