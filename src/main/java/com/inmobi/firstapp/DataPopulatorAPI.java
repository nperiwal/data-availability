package com.inmobi.firstapp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by oozie on 8/10/16.
 */
@Path("/")
public class DataPopulatorAPI {

    public static final String UNIFIED_TAG = "unified";

    public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return GSON.toJson("Welcome to Data availability Dashboard. Currently in Construction Phase!");

    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return Gives the Data Completeness Cache Dump
     */
    @Path("hourly/cache")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCompletenessCacheDump() {
        return GSON.toJson(AvailabilityCacheStore.getUnifiedCacheStore());
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return completeness of the set of measures based on the dataCompletenessTag for the given starttime and
     * endtime.
     */
    @Path("hourly/completeness")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDataCompleteness(@QueryParam("start_date") String startDate,
                                      @QueryParam("end_date") String endDate,
                                      @QueryParam("fact_tag") String factTag,
                                      @QueryParam("measure_tag") String measureTagString) {

        Map<String, Map<String, Float>> result = new HashMap<>();
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (startDate == null || endDate == null || factTag == null || measureTagString == null) {
            return GSON.toJson(result);
        }
        Set<String> measureTag = new HashSet<>(Arrays.asList(measureTagString.split(",")));
        if (factTag.equals(UNIFIED_TAG)) {
            Map<String, Map<Date, Float>> unifiedCompletenessCache = AvailabilityCacheStore.getUnifiedCacheStore();
            if (unifiedCompletenessCache == null) {
                System.out.println("Cache data not available currently");
                return GSON.toJson(result);
            }
            Calendar start = Calendar.getInstance();
            start.setTimeZone(TimeZone.getTimeZone("UTC"));
            try {
                start.setTime(formatter.parse(startDate));
            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println("");
                return GSON.toJson(result);
            }
            Calendar end = Calendar.getInstance();
            end.setTimeZone(TimeZone.getTimeZone("UTC"));
            try {
                end.setTime(formatter.parse(endDate));
            } catch (ParseException e) {
                e.printStackTrace();
                return GSON.toJson(result);
            }

            while(start.before(end)) {
                Date date = start.getTime();
                for (String tag : measureTag) {
                    if (unifiedCompletenessCache.containsKey(tag) && unifiedCompletenessCache.get(tag) != null &&
                            unifiedCompletenessCache.get(tag).containsKey(date)) {
                        if (result.get(tag) == null) {
                            result.put(tag, new TreeMap<String, Float>());
                        }
                        result.get(tag).put(formatter.format(date), unifiedCompletenessCache.get(tag).get(date) == null? 0f :
                                unifiedCompletenessCache.get(tag).get(date));
                    }
                }
                start.add(Calendar.HOUR_OF_DAY, 1);
            }
        }
        return GSON.toJson(result);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("hourly/today")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTodayAvailability(@DefaultValue("false") @QueryParam("debug") String debug) {
        int offset = 0;
        return getDayAvailabilityWithOffset(offset, debug);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("hourly/yesterday")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getYesterdayAvailability(@DefaultValue("false") @QueryParam("debug") String debug) {
        int offset = 1;
        return getDayAvailabilityWithOffset(offset, debug);

    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("hourly/day")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDayAvailability(@QueryParam("date") String full_day,
                                     @DefaultValue("false") @QueryParam("debug") String debug) {

        if (full_day == null) {
            return GSON.toJson("Please supply date query parameter");
        }

        String[] data = full_day.split("-");
        if (data.length != 3) {
            return GSON.toJson("Incorrect date parameter. It should be of type YYYY-MM-DD");
        }

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        String currentDayString = formatter.format(calendar.getTime());

        long offset;
        try {
            Date date1 = formatter.parse(currentDayString);
            Date date2 = formatter.parse(full_day);
            long diff = date1.getTime() - date2.getTime();
            offset = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            if (offset < 0) {
                return GSON.toJson("Future date is not allowed.");
            }
        } catch (ParseException e) {
            return GSON.toJson("Incorrect date parameter. It should be of type YYYY-MM-DD");
        }

        return getDayAvailabilityWithOffset((int)offset, debug);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("hourly/day-offset")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDayAvailabilityWithOffset(@QueryParam("offset") int offset,
                                               @DefaultValue("false") @QueryParam("debug") String debug) {

        Map<String, Result> resultMap = new TreeMap<>();

        String s = "";
        s += "offset: " + offset + "\n";
        s += "1\n";

        if (offset < 0) {
            return GSON.toJson("Future date is not allowed.");
        }

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DAY_OF_MONTH, -1 * offset);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String conduitDay = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd");
        String day = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd-HH");

        String fullHour = "";

        String auditRequest = "";
        String auditClick = "";
        String auditRender = "";
        String auditBilling = "";
        String auditConversion = "";

        Connection c;
        Statement stmt;
        try {

            s += "2\n";

            s+= day + "\n";

            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://opmd4002.grid.hkg1.inmobi.com:5499/conduit_audit",
                    "conduit_user", "C0n@uD!7+");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("select (timeinterval/(1000*60*60))%24 as hour," +
                    "sum(case when topic = 'rr' and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as request, " +
                    "sum(case when (topic like '%network_click_cp%_lhr1' or topic like '%network_click_cp%_uh1' or topic like '%network_click_cp%_hkg1' or topic like '%network_click_cp%_dfw1' or topic like '%network_click_invalid_lhr1' or topic like '%network_click_invalid_uh1' or topic like '%network_click_invalid_hkg1' or topic like '%network_click_invalid_dfw1') and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as click, " +
                    "sum(case when (topic like 'beacon_rr_lhr1_cp%_render' or topic like 'beacon_rr_uh1_cp%_render' or topic like 'beacon_rr_hkg1_cp%_render' or topic like 'beacon_rr_dfw1_cp%_render') and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as render, " +
                    "sum(case when (topic like 'billing_cp%_lhr1' or topic like 'billing_cp%_uh1' or topic like 'billing_cp%_hkg1' or topic like 'billing_cp%_dfw1') and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as billing, " +
                    "sum(case when (topic like 'adroit_report_obj_lhr1' or topic like 'adroit_report_obj_uh1' or topic like 'adroit_report_obj_hkg1' or topic like 'adroit_report_obj_dfw1') and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as conversion " +
                    "from daily_conduit_summary" + conduitDay + " group by (timeinterval/(1000*60*60))%24 order by (timeinterval/(1000*60*60))%24;");

            s += "3\n";
            while ( rs.next() ) {
                s += "4\n";
                int hour = Integer.parseInt(rs.getString("hour"));
                auditRequest = rs.getString("request");
                auditClick = rs.getString("click");
                auditRender = rs.getString("render");
                auditBilling = rs.getString("billing");
                auditConversion = rs.getString("conversion");

                auditRequest = modifyIfBlank(auditRequest);
                auditClick = modifyIfBlank(auditClick);
                auditRender = modifyIfBlank(auditRender);
                auditBilling = modifyIfBlank(auditBilling);
                auditConversion = modifyIfBlank(auditConversion);

                calendar.set(Calendar.HOUR_OF_DAY, hour);
                fullHour = formatter.format(calendar.getTime());
                Result result = new Result(fullHour, auditRequest, auditClick, auditRender, auditBilling,
                        auditConversion, "0", "0", "0", "0", "0", "NA", "NA", "NA", "NA", "NA");
                resultMap.put(fullHour, result);
                s += "5\n";
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            e.printStackTrace();
            s += e.getMessage() + "\n";
            s += "6\n";
        }

        String verticaRequest = "";
        String verticaClick = "";
        String verticaRender = "";
        String verticaBilling = "";
        String verticaConversion = "";
        Connection conn;
        try {
            Class.forName("com.vertica.Driver");
            conn = DriverManager.getConnection
                    ("jdbc:vertica://verticadb.uh1.inmobi.com:5433/verticadb", "hdfsload", "haswe4725load");

            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect.executeQuery
                    ("select event_received_time,sum(request_stream_count) as request,sum(click_stream_count) as click," +
                            "sum(render_stream_count) as render, sum(billing_stream_count) as billing," +
                            "sum(conversion_stream_count) as conversion from day_unified_availability_fact_v where event_received_time >= '"+ day +
                            " 00:00:00' and event_received_time <= '" + day + " 23:00:00' group by event_received_time order by event_received_time;");

            s += "7\n";
            while (myResult.next()) {
                s += "8\n";
                s += myResult.getString(1) + "\n";
                int hour = Integer.parseInt(myResult.getString(1).substring(11,13));
                verticaRequest = myResult.getString(2);
                verticaClick = myResult.getString(3);
                verticaRender = myResult.getString(4);
                verticaBilling = myResult.getString(5);
                verticaConversion = myResult.getString(6);

                verticaRequest = modifyIfBlank(verticaRequest);
                verticaClick = modifyIfBlank(verticaClick);
                verticaRender = modifyIfBlank(verticaRender);
                verticaBilling = modifyIfBlank(verticaBilling);
                verticaConversion = modifyIfBlank(verticaConversion);

                calendar.set(Calendar.HOUR_OF_DAY, hour);
                fullHour = formatter.format(calendar.getTime());
                Result result = resultMap.get(fullHour);

                result.setVerticaRequest(verticaRequest);
                result.setVerticaClick(verticaClick);
                result.setVerticaRender(verticaRender);
                result.setVerticaBilling(verticaBilling);
                result.setVerticaConversion(verticaConversion);

                s += "81\n";
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            s += "84\n";
            e.printStackTrace();
        }

        if (debug.equals("true")) {
            List<Result> resultList = new ArrayList<>();
            for (Map.Entry<String, Result> entry : resultMap.entrySet()) {
                Result result = entry.getValue();

                result.setRequestAvailability(calculate(result.getAuditRequest(), result.getVerticaRequest()));
                result.setClickAvailability(calculate(result.getAuditClick(), result.getVerticaClick()));
                result.setRenderAvailability(calculate(result.getAuditRender(), result.getVerticaRender()));
                result.setBillingAvailability(calculate(result.getAuditBilling(), result.getVerticaBilling()));
                result.setConversionAvailability(calculate(result.getAuditConversion(), result.getVerticaConversion()));

                resultList.add(result);
            }
            return GSON.toJson(resultList);
        }

        List<ModifiedResult> modifiedResults = new ArrayList<>();
        for (Map.Entry<String, Result> entry : resultMap.entrySet()) {
            Result result = entry.getValue();

            ModifiedResult modifiedResult = new ModifiedResult(result.getDate(), result.getVerticaRequest(),
                    result.getVerticaClick(), result.getVerticaRender(), result.getVerticaBilling(),
                    result.getVerticaConversion());

            modifiedResult.setRequestAvailability(modifyCalculate(result.getAuditRequest(), result.getVerticaRequest()));
            modifiedResult.setClickAvailability(modifyCalculate(result.getAuditClick(), result.getVerticaClick()));
            modifiedResult.setRenderAvailability(modifyCalculate(result.getAuditRender(), result.getVerticaRender()));
            modifiedResult.setBillingAvailability(modifyCalculate(result.getAuditBilling(), result.getVerticaBilling()));
            modifiedResult.setConversionAvailability(modifyCalculate(result.getAuditConversion(), result.getVerticaConversion()));

            modifiedResults.add(modifiedResult);
        }
        return GSON.toJson(modifiedResults);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("daily/today")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAggregatedTodayAvailability(@DefaultValue("false") @QueryParam("debug") String debug) {
        int offset = 0;
        return getAggregatedDayAvailabilityWithOffset(offset, debug);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("daily/yesterday")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAggregatedYesterdayAvailability(@DefaultValue("false") @QueryParam("debug") String debug) {
        int offset = 1;
        return getAggregatedDayAvailabilityWithOffset(offset, debug);

    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("daily/day")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAggregatedDayAvailability(@QueryParam("date") String full_day,
                                     @DefaultValue("false") @QueryParam("debug") String debug) {

        if (full_day == null) {
            return GSON.toJson("Please supply date query parameter");
        }

        String[] data = full_day.split("-");
        if (data.length != 3) {
            return GSON.toJson("Incorrect date parameter. It should be of type YYYY-MM-DD");
        }

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        String currentDayString = formatter.format(calendar.getTime());

        long offset;
        try {
            Date date1 = formatter.parse(currentDayString);
            Date date2 = formatter.parse(full_day);
            long diff = date1.getTime() - date2.getTime();
            offset = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            if (offset < 0) {
                return GSON.toJson("Future date is not allowed.");
            }
        } catch (ParseException e) {
            return GSON.toJson("Incorrect date parameter. It should be of type YYYY-MM-DD");
        }

        return getAggregatedDayAvailabilityWithOffset((int)offset, debug);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("daily/day-offset")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAggregatedDayAvailabilityWithOffset(@QueryParam("offset") int offset,
                                               @DefaultValue("false") @QueryParam("debug") String debug) {

        String s = "";
        s += "offset: " + offset + "\n";
        s += "1\n";

        if (offset < 0) {
            return GSON.toJson("Future date is not allowed.");
        }

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DAY_OF_MONTH, -1 * offset);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String conduitDay = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd");
        String day = formatter.format(calendar.getTime());

        String auditRequest = "";
        String auditClick = "";
        String auditRender = "";
        String auditBilling = "";
        String auditConversion = "";

        Connection c;
        Statement stmt;
        try {

            s += "2\n";

            s+= day + "\n";

            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://opmd4002.grid.hkg1.inmobi.com:5499/conduit_audit",
                    "conduit_user", "C0n@uD!7+");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("select " +
                    "sum(case when topic = 'rr' and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as request, " +
                    "sum(case when (topic like '%network_click_cp%_lhr1' or topic like '%network_click_cp%_uh1' or topic like '%network_click_cp%_hkg1' or topic like '%network_click_cp%_dfw1' or topic like '%network_click_invalid_lhr1' or topic like '%network_click_invalid_uh1' or topic like '%network_click_invalid_hkg1' or topic like '%network_click_invalid_dfw1') and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as click, " +
                    "sum(case when (topic like 'beacon_rr_lhr1_cp%_render' or topic like 'beacon_rr_uh1_cp%_render' or topic like 'beacon_rr_hkg1_cp%_render' or topic like 'beacon_rr_dfw1_cp%_render') and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as render, " +
                    "sum(case when (topic like 'billing_cp%_lhr1' or topic like 'billing_cp%_uh1' or topic like 'billing_cp%_hkg1' or topic like 'billing_cp%_dfw1') and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as billing, " +
                    "sum(case when (topic like 'adroit_report_obj_lhr1' or topic like 'adroit_report_obj_uh1' or topic like 'adroit_report_obj_hkg1' or topic like 'adroit_report_obj_dfw1') and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as conversion " +
                    "from daily_conduit_summary" + conduitDay + ";");

            s += "3\n";
            while ( rs.next() ) {
                s += "4\n";
                auditRequest = rs.getString("request");
                auditClick = rs.getString("click");
                auditRender = rs.getString("render");
                auditBilling = rs.getString("billing");
                auditConversion = rs.getString("conversion");

                auditRequest = modifyIfBlank(auditRequest);
                auditClick = modifyIfBlank(auditClick);
                auditRender = modifyIfBlank(auditRender);
                auditBilling = modifyIfBlank(auditBilling);
                auditConversion = modifyIfBlank(auditConversion);

                s += "5\n";
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            e.printStackTrace();
            s += e.getMessage() + "\n";
            s += "6\n";
        }

        String verticaRequest = "";
        String verticaClick = "";
        String verticaRender = "";
        String verticaBilling = "";
        String verticaConversion = "";
        Connection conn;
        try {
            Class.forName("com.vertica.Driver");
            conn = DriverManager.getConnection
                    ("jdbc:vertica://verticadb.uh1.inmobi.com:5433/verticadb", "hdfsload", "haswe4725load");

            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect.executeQuery
                    ("select sum(request_stream_count) as request,sum(click_stream_count) as click," +
                            "sum(render_stream_count) as render, sum(billing_stream_count) as billing," +
                            "sum(conversion_stream_count) as conversion from day_unified_availability_fact_v where event_received_time >= '"+ day +
                            " 00:00:00' and event_received_time <= '" + day + " 23:00:00';");

            s += "7\n";
            while (myResult.next()) {
                s += "8\n";
                verticaRequest = myResult.getString(1);
                verticaClick = myResult.getString(2);
                verticaRender = myResult.getString(3);
                verticaBilling = myResult.getString(4);
                verticaConversion = myResult.getString(5);

                verticaRequest = modifyIfBlank(verticaRequest);
                verticaClick = modifyIfBlank(verticaClick);
                verticaRender = modifyIfBlank(verticaRender);
                verticaBilling = modifyIfBlank(verticaBilling);
                verticaConversion = modifyIfBlank(verticaConversion);

                s += "81\n";
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            s += "84\n";
            e.printStackTrace();
        }


        Result result = new Result(day, auditRequest, auditClick, auditRender, auditBilling, auditConversion,
                "0", "0", "0", "0", "0", "NA", "NA", "NA", "NA", "NA");

        result.setVerticaRequest(verticaRequest);
        result.setVerticaClick(verticaClick);
        result.setVerticaRender(verticaRender);
        result.setVerticaBilling(verticaBilling);
        result.setVerticaConversion(verticaConversion);

        result.setRequestAvailability(calculate(result.getAuditRequest(), result.getVerticaRequest()));
        result.setClickAvailability(calculate(result.getAuditClick(), result.getVerticaClick()));
        result.setRenderAvailability(calculate(result.getAuditRender(), result.getVerticaRender()));
        result.setBillingAvailability(calculate(result.getAuditBilling(), result.getVerticaBilling()));
        result.setConversionAvailability(calculate(result.getAuditConversion(), result.getVerticaConversion()));

        if (debug.equals("true")) {
            return GSON.toJson(result);
        }


        ModifiedResult modifiedResult = new ModifiedResult(result.getDate(), result.getVerticaRequest(),
                result.getVerticaClick(), result.getVerticaRender(), result.getVerticaBilling(),
                result.getVerticaConversion());

        modifiedResult.setRequestAvailability(modifyCalculate(result.getAuditRequest(), result.getVerticaRequest()));
        modifiedResult.setClickAvailability(modifyCalculate(result.getAuditClick(), result.getVerticaClick()));
        modifiedResult.setRenderAvailability(modifyCalculate(result.getAuditRender(), result.getVerticaRender()));
        modifiedResult.setBillingAvailability(modifyCalculate(result.getAuditBilling(), result.getVerticaBilling()));
        modifiedResult.setConversionAvailability(modifyCalculate(result.getAuditConversion(), result.getVerticaConversion()));

        return GSON.toJson(modifiedResult);
    }

    private String modifyIfBlank(String input) {
        if (StringUtils.isBlank(input)) {
            return "0";
        } else {
            return input;
        }
    }



    private String calculate(String start, String end) {
        String availability = "NA";
        if (StringUtils.isNotBlank(start) && StringUtils.isNotBlank(end)) {
            Long s1 = Long.parseLong(start);
            Long s2 = Long.parseLong(end);
            if (s1 > 0) {
                double percent = Math.round((s2 * 100d / s1) * 100d) / 100d;
                availability = Double.toString(percent) + "%";
            }
        }
        return availability;
    }

    private String modifyCalculate(String start, String end) {
        String availability = "NA";
        if (StringUtils.isNotBlank(start) && StringUtils.isNotBlank(end)) {
            Long s1 = Long.parseLong(start);
            Long s2 = Long.parseLong(end);
            if (s1 > 0) {
                double percent = Math.round((s2 * 100d / s1) * 100d) / 100d;
                if (percent >= 100) {
                    percent = 100.0d;
                }
                availability = Double.toString(percent) + "%";
            }
        }
        return availability;
    }

    static class Result {
        String date;
        String auditRequest;
        String auditClick;
        String auditRender;
        String auditBilling;
        String auditConversion;
        String verticaRequest;
        String verticaClick;
        String verticaRender;
        String verticaBilling;
        String verticaConversion;
        String requestAvailability;
        String clickAvailability;
        String renderAvailability;
        String billingAvailability;
        String conversionAvailability;

        public void setVerticaRequest(String verticaRequest) {
            this.verticaRequest = verticaRequest;
        }

        public void setVerticaClick(String verticaClick) {
            this.verticaClick = verticaClick;
        }

        public void setVerticaRender(String verticaRender) {
            this.verticaRender = verticaRender;
        }

        public void setVerticaBilling(String verticaBilling) {
            this.verticaBilling = verticaBilling;
        }

        public void setVerticaConversion(String verticaConversion) {
            this.verticaConversion = verticaConversion;
        }

        public void setRequestAvailability(String requestAvailability) {
            this.requestAvailability = requestAvailability;
        }

        public void setClickAvailability(String clickAvailability) {
            this.clickAvailability = clickAvailability;
        }

        public void setRenderAvailability(String renderAvailability) {
            this.renderAvailability = renderAvailability;
        }

        public void setBillingAvailability(String billingAvailability) {
            this.billingAvailability = billingAvailability;
        }

        public void setConversionAvailability(String conversionAvailability) {
            this.conversionAvailability = conversionAvailability;
        }

        public String getDate() {
            return date;
        }

        public String getAuditRequest() {
            return auditRequest;
        }

        public String getAuditClick() {
            return auditClick;
        }

        public String getAuditRender() {
            return auditRender;
        }

        public String getAuditBilling() {
            return auditBilling;
        }

        public String getAuditConversion() {
            return auditConversion;
        }

        public String getVerticaRequest() {
            return verticaRequest;
        }

        public String getVerticaClick() {
            return verticaClick;
        }

        public String getVerticaRender() {
            return verticaRender;
        }

        public String getVerticaBilling() {
            return verticaBilling;
        }

        public String getVerticaConversion() {
            return verticaConversion;
        }

        public String getRequestAvailability() {
            return requestAvailability;
        }

        public String getClickAvailability() {
            return clickAvailability;
        }

        public String getRenderAvailability() {
            return renderAvailability;
        }

        public String getBillingAvailability() {
            return billingAvailability;
        }

        public String getConversionAvailability() {
            return conversionAvailability;
        }

        public Result(String date, String auditRequest, String auditClick, String auditRender, String auditBilling,
                      String auditConversion, String verticaRequest, String verticaClick, String verticaRender,
                      String verticaBilling, String verticaConversion, String requestAvailability,
                      String clickAvailability, String renderAvailability, String billingAvailability,
                      String conversionAvailability) {
            this.date = date;
            this.auditRequest = auditRequest;
            this.auditClick = auditClick;
            this.auditRender = auditRender;
            this.auditBilling = auditBilling;
            this.auditConversion = auditConversion;
            this.verticaRequest = verticaRequest;
            this.verticaClick = verticaClick;
            this.verticaRender = verticaRender;
            this.verticaBilling = verticaBilling;
            this.verticaConversion = verticaConversion;
            this.requestAvailability = requestAvailability;
            this.clickAvailability = clickAvailability;
            this.renderAvailability = renderAvailability;
            this.billingAvailability = billingAvailability;
            this.conversionAvailability = conversionAvailability;
        }
    }

    static class ModifiedResult {
        String date;
        String verticaRequest;
        String verticaClick;
        String verticaRender;
        String verticaBilling;
        String verticaConversion;
        String requestAvailability;
        String clickAvailability;
        String renderAvailability;
        String billingAvailability;
        String conversionAvailability;

        public ModifiedResult(String date, String verticaRequest, String verticaClick, String verticaRender,
                              String verticaBilling, String verticaConversion) {
            this.date = date;
            this.verticaRequest = verticaRequest;
            this.verticaClick = verticaClick;
            this.verticaRender = verticaRender;
            this.verticaBilling = verticaBilling;
            this.verticaConversion = verticaConversion;
        }

        public void setRequestAvailability(String requestAvailability) {
            this.requestAvailability = requestAvailability;
        }

        public void setClickAvailability(String clickAvailability) {
            this.clickAvailability = clickAvailability;
        }

        public void setRenderAvailability(String renderAvailability) {
            this.renderAvailability = renderAvailability;
        }

        public void setBillingAvailability(String billingAvailability) {
            this.billingAvailability = billingAvailability;
        }

        public void setConversionAvailability(String conversionAvailability) {
            this.conversionAvailability = conversionAvailability;
        }
    }

}
