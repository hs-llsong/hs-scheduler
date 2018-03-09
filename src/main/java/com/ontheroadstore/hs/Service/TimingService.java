package com.ontheroadstore.hs.Service;

import java.util.concurrent.TimeUnit;

/**
 * Created by Jeffrey(zuoyaofei@icloud.com) on 18/3/9.
 */
public class TimingService implements ScheduledExecutorService {
    @Override
    public boolean schedule(Runnable command, long delay, TimeUnit unit) {
        return false;
    }
}
