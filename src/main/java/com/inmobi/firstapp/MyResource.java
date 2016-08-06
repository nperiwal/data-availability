package com.inmobi.firstapp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("/")
public class MyResource {

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
     * @return String that will be returned as an application/json response.
     */
    @Path("hour")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getHourlyAvailability(@QueryParam("full_hour") String fullHour, @QueryParam("stream") String stream) {

        String[] data = fullHour.split("-");
        if (data.length != 4) {
            return GSON.toJson("Incorrect parameter full_hour. It should be of type YYYY-MM-DD-HH");
        }
        String day = data[0] + data[1] + data[2];
        String hour = data[3];

        String auditSum = "";
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://opmd4002.grid.hkg1.inmobi.com:5499/conduit_audit",
                            "conduit_user", "C0n@uD!7+");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery( "select sum(c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + " +
                    "c15 + c30 + c60 + c120 + c240 + c600) from daily_conduit_summary" + day + " where topic like 'rr' " +
                    "and tier = 'LOCAL' and (timeinterval/(1000*60*60))%24=" + hour + ";" );
            while ( rs.next() ) {
                auditSum = rs.getString("sum");
                if (StringUtils.isBlank(auditSum)) {
                    auditSum = "0";
                }
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        String verticaSum = "";
        Connection conn;
        try {
            Class.forName("com.vertica.Driver");
            conn = DriverManager.getConnection
                    ("jdbc:vertica://verticadb.uh1.inmobi.com:5433/verticadb", "hdfsload", "haswe4725load");

            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect.executeQuery
                    ("select sum(page_requests) from hour_supply_fact where event_time='"+ day + " " + hour + ":00:00';");

            while (myResult.next()) {
                verticaSum = myResult.getString(1);
                if (StringUtils.isBlank(verticaSum)) {
                    verticaSum = "0";
                }
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String availability = "NA";
        if (StringUtils.isNotBlank(auditSum)) {
            Long s1 =  Long.parseLong(auditSum);
            Long s2 = Long.parseLong(verticaSum);
            if (s1 > 0) {
                double percent = Math.round((s2*100d/s1)*100d)/100d;
                availability = Double.toString(percent) + "%";
            }
        }

        Result result = new Result(fullHour, auditSum, verticaSum, availability);
        return GSON.toJson(result);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("today")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTodayAvailability(@QueryParam("stream") String stream) {

        String s = "";

        Map<String, Result> resultMap = new TreeMap<>();

        s += "1\n";

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(new Date(System.currentTimeMillis()));
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        //formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        String conduitDay = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd");
        //formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        String day = formatter.format(calendar.getTime());
        int currentHour = calendar.get(Calendar.HOUR_OF_DAY);

        formatter = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        //formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        String verticaHour = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd-HH");
        //formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        String fullHour = "";
        String auditSum = "";
        Connection c = null;
        Statement stmt = null;
        try {

            s += "2\n";

            s+= day + "\n";

            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://opmd4002.grid.hkg1.inmobi.com:5499/conduit_audit",
                    "conduit_user", "C0n@uD!7+");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("select sum(c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + " +
                    "c15 + c30 + c60 + c120 + c240 + c600) from daily_conduit_summary" + conduitDay + " where topic like " +
                    "'rr' and tier='LOCAL' and (timeinterval/(1000*60*60))%24 >= 0 and " +
                    "(timeinterval/(1000*60*60))%24 <= " + currentHour + " group by (timeinterval/(1000*60*60))%24 " +
                    "order by (timeinterval/(1000*60*60))%24;");

            s += "3\n";
            int hour = 0;
            while ( rs.next() ) {
                s += "4\n";
                auditSum = rs.getString("sum");
                if (StringUtils.isBlank(auditSum)) {
                    auditSum = "0";
                }
                calendar.set(Calendar.HOUR_OF_DAY, hour++);
                fullHour = formatter.format(calendar.getTime());
                Result result = new Result(fullHour, auditSum, "", "NA");
                resultMap.put(fullHour, result);
                s += "5\n";
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            e.printStackTrace();
            s += e.getMessage() + "\n";
            s += e.getCause() + "\n";
            s += "6\n";
        }

        String verticaSum = "";
        Connection conn;
        try {
            Class.forName("com.vertica.Driver");
            conn = DriverManager.getConnection
                    ("jdbc:vertica://verticadb.uh1.inmobi.com:5433/verticadb", "hdfsload", "haswe4725load");

            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect.executeQuery
                    ("select sum(page_requests) from hour_supply_fact where event_time>= '"+ day + " 00:00:00' and " +
                            "event_time<= '" + verticaHour + "' group by event_time order by event_time;");

            int hour = 0;
            while (myResult.next()) {
                verticaSum = myResult.getString(1);
                if (StringUtils.isBlank(verticaSum)) {
                    verticaSum = "0";
                }
                calendar.set(Calendar.HOUR_OF_DAY, hour++);
                fullHour = formatter.format(calendar.getTime());
                Result result = resultMap.get(fullHour);
                result.setVertica(verticaSum);
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<Result> resultList = new ArrayList<>();
        for (Map.Entry<String, Result> entry : resultMap.entrySet()) {
            Result result = entry.getValue();

            String availability = "NA";
            if (StringUtils.isNotBlank(result.getAudit()) && StringUtils.isNotBlank(result.getVertica())) {
                Long s1 =  Long.parseLong(result.getAudit());
                Long s2 = Long.parseLong(result.getVertica());
                if (s1 > 0) {
                    double percent = Math.round((s2*100d/s1)*100d)/100d;
                    availability = Double.toString(percent) + "%";
                }
            }
            result.setAvailability(availability);
            resultList.add(result);
        }
        s += "9\n";
        return GSON.toJson(resultList);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("yesterday")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getYesterdayAvailability(@QueryParam("stream") String stream) {

        String s = "";

        Map<String, Result> resultMap = new TreeMap<>();

        s += "1\n";

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String conduitDay = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd");
        String day = formatter.format(calendar.getTime());

        formatter = new SimpleDateFormat("yyyy-MM-dd-HH");

        String fullHour = "";
        String auditSum = "";
        Connection c = null;
        Statement stmt = null;
        try {

            s += "2\n";

            s+= day + "\n";

            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://opmd4002.grid.hkg1.inmobi.com:5499/conduit_audit",
                    "conduit_user", "C0n@uD!7+");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("select sum(c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + " +
                    "c15 + c30 + c60 + c120 + c240 + c600) from daily_conduit_summary" + conduitDay + " where topic like " +
                    "'rr' and tier='LOCAL' group by (timeinterval/(1000*60*60))%24 order by (timeinterval/(1000*60*60))%24;");

            s += "3\n";
            int hour = 0;
            while ( rs.next() ) {
                s += "4\n";
                auditSum = rs.getString("sum");
                if (StringUtils.isBlank(auditSum)) {
                    auditSum = "0";
                }
                calendar.set(Calendar.HOUR_OF_DAY, hour++);
                fullHour = formatter.format(calendar.getTime());
                Result result = new Result(fullHour, auditSum, "", "NA");
                resultMap.put(fullHour, result);
                s += "5\n";
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            e.printStackTrace();
            s += e.getMessage() + "\n";
            s += e.getCause() + "\n";
            s += "6\n";
        }

        String verticaSum = "";
        Connection conn;
        try {
            Class.forName("com.vertica.Driver");
            conn = DriverManager.getConnection
                    ("jdbc:vertica://verticadb.uh1.inmobi.com:5433/verticadb", "hdfsload", "haswe4725load");

            Statement mySelect = conn.createStatement();
            ResultSet myResult = mySelect.executeQuery
                    ("select sum(page_requests) from hour_supply_fact where event_time>= '"+ day + " 00:00:00' and " +
                            "event_time<= '" + day + " 23:00:00' group by event_time order by event_time;");

            int hour = 0;
            while (myResult.next()) {
                verticaSum = myResult.getString(1);
                if (StringUtils.isBlank(verticaSum)) {
                    verticaSum = "0";
                }
                calendar.set(Calendar.HOUR_OF_DAY, hour++);
                fullHour = formatter.format(calendar.getTime());
                Result result = resultMap.get(fullHour);
                result.setVertica(verticaSum);
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<Result> resultList = new ArrayList<>();
        for (Map.Entry<String, Result> entry : resultMap.entrySet()) {
            Result result = entry.getValue();

            String availability = "NA";
            if (StringUtils.isNotBlank(result.getAudit()) && StringUtils.isNotBlank(result.getVertica())) {
                Long s1 =  Long.parseLong(result.getAudit());
                Long s2 = Long.parseLong(result.getVertica());
                if (s1 > 0) {
                    double percent = Math.round((s2*100d/s1)*100d)/100d;
                    availability = Double.toString(percent) + "%";
                }
            }
            result.setAvailability(availability);
            resultList.add(result);
        }
        s += "9\n";
        return GSON.toJson(resultList);
    }

    static class Result {
        String date;
        String audit;
        String vertica;
        String availability;

        public String getAudit() {
            return audit;
        }

        public void setAudit(String audit) {
            this.audit = audit;
        }

        public String getVertica() {
            return vertica;
        }

        public void setVertica(String vertica) {
            this.vertica = vertica;
        }

        public void setAvailability(String availability) {
            this.availability = availability;
        }

        public Result(String date, String audit, String vertica, String availability) {
            this.date = date;
            this.audit = audit;
            this.vertica = vertica;
            this.availability = availability;
        }
    }

}
