package com.inmobi.firstapp;

import javax.servlet.*;
import java.util.*;

/**
 * Created by oozie on 11/3/16.
 */
public class PurgeCacheStore extends TimerTask implements ServletContextListener {

    public static final long ONE_SECOND = 1000;
    public static final long FIVE_MINUTE = 5 * 60 * ONE_SECOND;
    public static final long ONE_DAY =  1 * 24 * 60 * 60 * ONE_SECOND;
    private static TimerTask purgeTask;

    public static void main(String args[]) {
        purgeTask = new PurgeCacheStore();
        Timer timer = new Timer();
        timer.schedule(purgeTask, FIVE_MINUTE, ONE_DAY);
    }

    @Override
    public void run() {
        int RETENTION_DAYS = 4;
        System.out.println("Purge Task started at:" + new Date());
        purgeStore(RETENTION_DAYS);
        System.out.println("Purge Task finished at:" + new Date());
    }

    private void purgeStore(int days) {

        Map<String, Map<Date, Float>> unifiedCompletenessCache = AvailabilityCacheStore.getUnifiedCacheStore();
        if (unifiedCompletenessCache == null) {
            System.out.println("Cache not initialized yet!");
            return;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DAY_OF_MONTH, -1 * days);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        for (Map<Date, Float> completenessMap : unifiedCompletenessCache.values()) {
            for (Date date : completenessMap.keySet()) {
                if (date.before(calendar.getTime())) {
                    completenessMap.remove(date);
                }
            }
        }
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        System.out.println("PurgeCacheStore: ServletContextListener started");
        main(null);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        System.out.println("PurgeCacheStore: ServletContextListener destroyed");
        if (purgeTask != null) {
            purgeTask.cancel();
        }
    }
}
