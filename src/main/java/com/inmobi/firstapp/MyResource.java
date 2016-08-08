package com.inmobi.firstapp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
    @Path("{stream}/hourly/hour")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getHourlyAvailability(@QueryParam("full_hour") String fullHour, @PathParam("stream") String stream) {

        if (fullHour == null) {
            return GSON.toJson("Please supply full_hour query parameter");
        }

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
    @Path("{stream}/hourly/today")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTodayAvailability1(@PathParam("stream") String stream) {
        int offset = 0;
        return getDayAvailabilityWithOffset(offset, stream);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/hourly/yesterday")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getYesterdayAvailability1(@PathParam("stream") String stream) {
        int offset = 1;
        return getDayAvailabilityWithOffset(offset, stream);

    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/hourly/day")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDayAvailability(@QueryParam("date") String full_day, @PathParam("stream") String stream) {

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

        return getDayAvailabilityWithOffset((int)offset, stream);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/hourly/offset")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDayAvailabilityWithOffset(@QueryParam("offset") int offset, @PathParam("stream") String stream) {

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
            ResultSet rs = stmt.executeQuery("select (timeinterval/(1000*60*60))%24 as hour,sum(c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + " +
                    "c15 + c30 + c60 + c120 + c240 + c600) from daily_conduit_summary" + conduitDay + " where topic like " +
                    "'rr' and tier='LOCAL' group by (timeinterval/(1000*60*60))%24 order by (timeinterval/(1000*60*60))%24;");

            s += "3\n";
            while ( rs.next() ) {
                s += "4\n";
                int hour = Integer.parseInt(rs.getString("hour"));
                auditSum = rs.getString("sum");
                if (StringUtils.isBlank(auditSum)) {
                    auditSum = "0";
                }
                calendar.set(Calendar.HOUR_OF_DAY, hour);
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
                    ("select event_time,sum(page_requests) from hour_supply_fact where event_time >= '"+ day + " 00:00:00' and " +
                            "event_time <= '" + day + " 23:00:00' group by event_time order by event_time;");

            s += "7\n";
            while (myResult.next()) {
                s += "8\n";
                s += myResult.getString(1) + "\n";
                int hour = Integer.parseInt(myResult.getString(1).substring(11,13));
                verticaSum = myResult.getString(2);
                if (StringUtils.isBlank(verticaSum)) {
                    verticaSum = "0";
                }
                calendar.set(Calendar.HOUR_OF_DAY, hour);
                fullHour = formatter.format(calendar.getTime());
                Result result = resultMap.get(fullHour);
                result.setVertica(verticaSum);
                s += "81\n";
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            s += "84\n";
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
        //return s;
        return GSON.toJson(resultList);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/daily/today")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTodayDailyAvailability1(@PathParam("stream") String stream) {
        int offset = 0;
        return getAggregatedDayAvailabilityWithOffset(offset, stream);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/daily/yesterday")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getYesterdayDailyAvailability1(@PathParam("stream") String stream) {
        int offset = 1;
        return getAggregatedDayAvailabilityWithOffset(offset, stream);

    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/daily/day")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAggregatedDayAvailability(@QueryParam("date") String full_day, @PathParam("stream") String stream) {

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

        return getAggregatedDayAvailabilityWithOffset((int)offset, stream);
    }

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     *
     * @return String that will be returned as an application/json response.
     */
    @Path("{stream}/daily/day-offset")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAggregatedDayAvailabilityWithOffset(@QueryParam("offset") int offset, @PathParam("stream") String stream) {

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
                    "'rr' and tier='LOCAL';");

            s += "3\n";
            while ( rs.next() ) {
                s += "4\n";
                auditSum = rs.getString("sum");
                if (StringUtils.isBlank(auditSum)) {
                    auditSum = "0";
                }
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
                    ("select sum(page_requests) from hour_supply_fact where event_time >= '"+ day + " 00:00:00' and " +
                            "event_time <= '" + day + " 23:00:00';");

            s += "7\n";
            while (myResult.next()) {
                s += "8\n";
                s += myResult.getString(1) + "\n";
                verticaSum = myResult.getString(1);
                if (StringUtils.isBlank(verticaSum)) {
                    verticaSum = "0";
                }
                s += "81\n";
            }
            mySelect.close();
            conn.close();
        } catch (Exception e) {
            s += "84\n";
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

        Result result = new Result(day, auditSum, verticaSum, availability);
        s += "9\n";
        //return s;
        return GSON.toJson(result);
    }

    static class Result {
        String date;
        String audit;
        String vertica;
        String availability;

        public String getAudit() {
            return audit;
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
