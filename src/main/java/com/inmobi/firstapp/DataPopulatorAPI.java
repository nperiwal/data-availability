package com.inmobi.firstapp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by oozie on 8/10/16.
 */
@Path("/")
public class DataPopulatorAPI {

    public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

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
        String auditRenderCPC = "";
        String auditRenderCPM = "";
        String auditBilling = "";

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
                    "sum(case when topic like '%network_click_cp%_lhr1' or topic like '%network_click_cp%_uh1' or topic like '%network_click_cp%_hkg1' or topic like '%network_click_cp%_dfw1' and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as click, " +
                    "sum(case when topic like 'beacon_rr_lhr1_cpc_render' or topic like 'beacon_rr_uh1_cpc_render' or topic like 'beacon_rr_hkg1_cpc_render' or topic like 'beacon_rr_dfw1_cpc_render' and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as rendercpc, " +
                    "sum(case when topic like 'beacon_rr_lhr1_cpm_render' or topic like 'beacon_rr_uh1_cpm_render' or topic like 'beacon_rr_hkg1_cpm_render' or topic like 'beacon_rr_dfw1_cpm_render' and tier='LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as rendercpm, " +
                    "sum(case when topic like 'billing_cp%_lhr1' or topic like 'billing_cp%_uh1' or topic like 'billing_cp%_hkg1' or topic like 'billing_cp%_dfw1' and tier = 'LOCAL' then (c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c15 + c30 + c60 + c120 + c240 + c600) end) as billing " +
                    "from daily_conduit_summary" + conduitDay + " group by (timeinterval/(1000*60*60))%24 order by (timeinterval/(1000*60*60))%24;");

            s += "3\n";
            while ( rs.next() ) {
                s += "4\n";
                int hour = Integer.parseInt(rs.getString("hour"));
                auditRequest = rs.getString("request");
                auditClick = rs.getString("click");
                auditRenderCPC = rs.getString("rendercpc");
                auditRenderCPM = rs.getString("rendercpm");
                auditBilling = rs.getString("billing");

                auditRequest = modifyIfBlank(auditRequest);
                auditClick = modifyIfBlank(auditClick);
                auditRenderCPC = modifyIfBlank(auditRenderCPC);
                auditRenderCPM = modifyIfBlank(auditRenderCPM);
                auditBilling = modifyIfBlank(auditBilling);

                calendar.set(Calendar.HOUR_OF_DAY, hour);
                fullHour = formatter.format(calendar.getTime());
                Result result = new Result(fullHour, auditRequest, auditClick, auditRenderCPC, auditRenderCPM,
                        auditBilling, "0", "0", "0", "0", "0", "NA", "NA", "NA", "NA", "NA");
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
        String verticaRenderCPC = "";
        String verticaRenderCPM = "";
        String verticaBilling = "";
        Connection conn;
        try {
            Class.forName("com.vertica.Driver");
            conn = DriverManager.getConnection
                    ("jdbc:vertica://db1001.ver.uh1.inmobi.com:5433/verticadb", "verticauser", "vtwrite#123");

            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect.executeQuery
                    ("select event_time,sum(request_stream_count) as request,sum(click_stream_count) as click," +
                            "sum(rendered_stream_count_cpc) as rendercpc,sum(rendered_stream_count_cpm) as rendercpm," +
                            "sum(billing_stream_count) as billing from unified_reporting_summary where event_time >= '"+ day +
                            " 00:00:00' and event_time <= '" + day + " 23:00:00' group by event_time order by event_time;");

            s += "7\n";
            while (myResult.next()) {
                s += "8\n";
                s += myResult.getString(1) + "\n";
                int hour = Integer.parseInt(myResult.getString(1).substring(11,13));
                verticaRequest = myResult.getString(2);
                verticaClick = myResult.getString(3);
                verticaRenderCPC = myResult.getString(4);
                verticaRenderCPM = myResult.getString(5);
                verticaBilling = myResult.getString(6);

                verticaRequest = modifyIfBlank(verticaRequest);
                verticaClick = modifyIfBlank(verticaClick);
                verticaRenderCPC = modifyIfBlank(verticaRenderCPC);
                verticaRenderCPM = modifyIfBlank(verticaRenderCPM);
                verticaBilling = modifyIfBlank(verticaBilling);

                calendar.set(Calendar.HOUR_OF_DAY, hour);
                fullHour = formatter.format(calendar.getTime());
                Result result = resultMap.get(fullHour);

                result.setVerticaRequest(verticaRequest);
                result.setVerticaClick(verticaClick);
                result.setVerticaRenderCPC(verticaRenderCPC);
                result.setVerticaRenderCPM(verticaRenderCPM);
                result.setVerticaBilling(verticaBilling);

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
                result.setRenderCPCAvailability(calculate(result.getAuditRenderCPC(), result.getVerticaRenderCPC()));
                result.setRenderCPMAvailability(calculate(result.getAuditRenderCPM(), result.getVerticaRenderCPM()));
                result.setBillingAvailability(calculate(result.getAuditBilling(), result.getVerticaBilling()));

                resultList.add(result);
            }
            return GSON.toJson(resultList);
        }

        List<ModifiedResult> modifiedResults = new ArrayList<>();
        for (Map.Entry<String, Result> entry : resultMap.entrySet()) {
            Result result = entry.getValue();

            ModifiedResult modifiedResult = new ModifiedResult(result.getDate(), result.getVerticaRequest(),
                    result.getVerticaClick(), result.getVerticaRenderCPC(), result.getVerticaRenderCPM(),
                    result.getVerticaBilling());

            modifiedResult.setRequestAvailability(modifyCalculate(result.getAuditRequest(), result.getVerticaRequest()));
            modifiedResult.setClickAvailability(modifyCalculate(result.getAuditClick(), result.getVerticaClick()));
            modifiedResult.setRenderCPCAvailability(modifyCalculate(result.getAuditRenderCPC(), result.getVerticaRenderCPC()));
            modifiedResult.setRenderCPMAvailability(modifyCalculate(result.getAuditRenderCPM(), result.getVerticaRenderCPM()));
            modifiedResult.setBillingAvailability(modifyCalculate(result.getAuditBilling(), result.getVerticaBilling()));

            modifiedResults.add(modifiedResult);
        }
        return GSON.toJson(modifiedResults);
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
        String auditRenderCPC;
        String auditRenderCPM;
        String auditBilling;
        String verticaRequest;
        String verticaClick;
        String verticaRenderCPC;
        String verticaRenderCPM;
        String verticaBilling;
        String requestAvailability;
        String clickAvailability;
        String renderCPCAvailability;
        String renderCPMAvailability;
        String billingAvailability;

        public void setVerticaRequest(String verticaRequest) {
            this.verticaRequest = verticaRequest;
        }

        public void setVerticaClick(String verticaClick) {
            this.verticaClick = verticaClick;
        }

        public void setVerticaRenderCPC(String verticaRenderCPC) {
            this.verticaRenderCPC = verticaRenderCPC;
        }

        public void setVerticaRenderCPM(String verticaRenderCPM) {
            this.verticaRenderCPM = verticaRenderCPM;
        }

        public void setVerticaBilling(String verticaBilling) {
            this.verticaBilling = verticaBilling;
        }

        public void setRequestAvailability(String requestAvailability) {
            this.requestAvailability = requestAvailability;
        }

        public void setClickAvailability(String clickAvailability) {
            this.clickAvailability = clickAvailability;
        }

        public void setRenderCPCAvailability(String renderCPCAvailability) {
            this.renderCPCAvailability = renderCPCAvailability;
        }

        public void setRenderCPMAvailability(String renderCPMAvailability) {
            this.renderCPMAvailability = renderCPMAvailability;
        }

        public void setBillingAvailability(String billingAvailability) {
            this.billingAvailability = billingAvailability;
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

        public String getAuditRenderCPC() {
            return auditRenderCPC;
        }

        public String getAuditRenderCPM() {
            return auditRenderCPM;
        }

        public String getAuditBilling() {
            return auditBilling;
        }

        public String getVerticaRequest() {
            return verticaRequest;
        }

        public String getVerticaClick() {
            return verticaClick;
        }

        public String getVerticaRenderCPC() {
            return verticaRenderCPC;
        }

        public String getVerticaRenderCPM() {
            return verticaRenderCPM;
        }

        public String getVerticaBilling() {
            return verticaBilling;
        }

        public String getRequestAvailability() {
            return requestAvailability;
        }

        public String getClickAvailability() {
            return clickAvailability;
        }

        public String getRenderCPCAvailability() {
            return renderCPCAvailability;
        }

        public String getRenderCPMAvailability() {
            return renderCPMAvailability;
        }

        public String getBillingAvailability() {
            return billingAvailability;
        }

        public Result(String date, String auditRequest, String auditClick, String auditRenderCPC,
                      String auditRenderCPM, String auditBilling, String verticaRequest, String verticaClick,
                      String verticaRenderCPC, String verticaRenderCPM, String verticaBilling,
                      String requestAvailability, String clickAvailability, String renderCPCAvailability,
                      String renderCPMAvailability, String billingAvailability) {
            this.date = date;
            this.auditRequest = auditRequest;
            this.auditClick = auditClick;
            this.auditRenderCPC = auditRenderCPC;
            this.auditRenderCPM = auditRenderCPM;
            this.auditBilling = auditBilling;
            this.verticaRequest = verticaRequest;
            this.verticaClick = verticaClick;
            this.verticaRenderCPC = verticaRenderCPC;
            this.verticaRenderCPM = verticaRenderCPM;
            this.verticaBilling = verticaBilling;
            this.requestAvailability = requestAvailability;
            this.clickAvailability = clickAvailability;
            this.renderCPCAvailability = renderCPCAvailability;
            this.renderCPMAvailability = renderCPMAvailability;
            this.billingAvailability = billingAvailability;
        }
    }

    static class ModifiedResult {
        String date;
        String verticaRequest;
        String verticaClick;
        String verticaRenderCPC;
        String verticaRenderCPM;
        String verticaBilling;
        String requestAvailability;
        String clickAvailability;
        String renderCPCAvailability;
        String renderCPMAvailability;
        String billingAvailability;

        public ModifiedResult(String date, String verticaRequest, String verticaClick, String verticaRenderCPC,
                              String verticaRenderCPM, String verticaBilling) {
            this.date = date;
            this.verticaRequest = verticaRequest;
            this.verticaClick = verticaClick;
            this.verticaRenderCPC = verticaRenderCPC;
            this.verticaRenderCPM = verticaRenderCPM;
            this.verticaBilling = verticaBilling;
        }

        public void setRequestAvailability(String requestAvailability) {
            this.requestAvailability = requestAvailability;
        }

        public void setClickAvailability(String clickAvailability) {
            this.clickAvailability = clickAvailability;
        }

        public void setRenderCPCAvailability(String renderCPCAvailability) {
            this.renderCPCAvailability = renderCPCAvailability;
        }

        public void setRenderCPMAvailability(String renderCPMAvailability) {
            this.renderCPMAvailability = renderCPMAvailability;
        }

        public void setBillingAvailability(String billingAvailability) {
            this.billingAvailability = billingAvailability;
        }
    }

}
