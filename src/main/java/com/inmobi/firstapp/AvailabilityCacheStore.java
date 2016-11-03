package com.inmobi.firstapp;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.inmobi.firstapp.DataPopulatorAPI.Result;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.lang.reflect.Type;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;

public class AvailabilityCacheStore extends TimerTask implements ServletContextListener {

   enum Stream {

        REQUEST("request"),
        CLICK("click"),
        RENDER("render"),
        CONVERSION("conversion"),
        BILLING("billing");

        private final String name;

        Stream(String s) {
            this.name = s;
        }

        public String getName() {
            return name;
        }
    }

    public static Map<String, Map<Date, Float>> unifiedCacheStore = new ConcurrentHashMap<>();

    public static final long ONE_SECOND = 1000;
    public static final long THIRTY_MINUTE = 30 * 60 * ONE_SECOND;
    private static TimerTask timerTask;

    public static Map<String, Map<Date, Float>> getUnifiedCacheStore() {
        return unifiedCacheStore;
    }

    public static void main(String args[]) {
        timerTask = new AvailabilityCacheStore();
        Timer timer = new Timer();
        timer.schedule(timerTask, THIRTY_MINUTE, THIRTY_MINUTE);
    }

    @Override
    public void run() {
        System.out.println("Task started at:" + new Date());
        completeTask(2);
        System.out.println("Task finished at:" + new Date());
    }

    private void completeTask(int numIter) {
        String output = "";
        DataPopulatorAPI dataPopulatorAPI = new DataPopulatorAPI();
        for (int i=0; i<numIter; i++) {
            output = dataPopulatorAPI.getDayAvailabilityWithOffset(i, "false");
            populateStore(output);
        }
    }

    private void populateStore(String output) {
        if (output == null || output.isEmpty()) {
            System.out.println("Null or Empty output");
            return;
        }
        Type listType = new TypeToken<ArrayList<Result>>(){}.getType();
        List<Result> resultList = new Gson().fromJson (output, listType);

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (Result result : resultList) {
            try {
                for (Stream stream : Stream.values()) {
                    if (unifiedCacheStore.get(stream.getName()) == null) {
                        unifiedCacheStore.put(stream.getName(), new ConcurrentSkipListMap<Date, Float>());
                    }
                    unifiedCacheStore.get(stream.getName()).put(formatter.parse(result.getDate()),
                            Float.parseFloat(modify(result, stream)));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    private static String modify(Result result, Stream stream) {
        String response = null;
        switch (stream) {
            case REQUEST:
                response = result.getRequestAvailability();
                break;
            case RENDER:
                response =  result.getRenderAvailability();
                break;
            case CLICK:
                response = result.getClickAvailability();
                break;
            case BILLING:
                response = result.getBillingAvailability();
                break;
            case CONVERSION:
                response = result.getConversionAvailability();
                break;
        }

        if (!response.equals("NA")) {
            return response.substring(0, response.length()-1);
        } else {
            return "0";
        }
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        System.out.println("AvailabilityCacheStore: ServletContextListener started");
        completeTask(5);
        main(null);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        System.out.println("AvailabilityCacheStore: ServletContextListener destroyed");
        if (timerTask != null) {
            timerTask.cancel();
        }
    }
}
