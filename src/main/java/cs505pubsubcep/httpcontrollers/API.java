package cs505pubsubcep.httpcontrollers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import cs505pubsubcep.DatabaseSetup;
import cs505pubsubcep.Launcher;
import cs505pubsubcep.CEP.accessRecord;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    private String[] alertZipList;

    public DatabaseSetup dbSetup = new DatabaseSetup();

    public API() {
        gson = new Gson();
    }

    // check local
    // curl --header "X-Auth-API-key:1234" "http://localhost:8088/api/checkmycep"

    // check remote
    // curl --header "X-Auth-API-key:1234"
    // "http://[linkblueid].cs.uky.edu:8082/api/checkmycep"
    // curl --header "X-Auth-API-key:1234" "http://localhost:8081/api/checkmycep"

    // check remote
    // curl --header "X-Auth-API-key:1234"
    // "http://[linkblueid].cs.uky.edu:8081/api/checkmycep"

    @GET
    @Path("/checkmycep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkMyEndpoint(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            // get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            // get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            Map<String, String> responseMap = new HashMap<>();
            if (Launcher.cepEngine != null) {

                responseMap.put("success", Boolean.TRUE.toString());
                responseMap.put("status_desc", "CEP Engine exists");

            } else {
                responseMap.put("success", Boolean.FALSE.toString());
                responseMap.put("status_desc", "CEP Engine is null!");
            }

            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getaccesscount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            // get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            // get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            // generate event based on access
            String inputEvent = gson.toJson(new accessRecord(remoteIP, access_ts));
            System.out.println("inputEvent: " + inputEvent);

            // send input event to CEP
            Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent);

            // generate a response
            Map<String, String> responseMap = new HashMap<>();
            responseMap.put("accesscoint", String.valueOf(Launcher.accessCount));
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // Application Management Functions
    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeam(@HeaderParam("X-Auth-API-Key") String authKey) {
        String teamName = "404 Team Name Not Found";
        String[] teamMemberSids = { "12145986", "12062818" };
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();
        int appStatusCode = 0;

        try (Socket s = new Socket("localhost", 8088)) {
            appStatusCode = 1; // if app is online set status code to 1
        } catch (Exception e) {
            System.out.println(e);
        }

        responseMap.put("team_name", teamName);
        responseMap.put("Team_members_sids", teamMemberSids);
        responseMap.put("app_status_code", String.valueOf(appStatusCode));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset(@HeaderParam("X-Auth-API-Key") String authKey) {
        int resetStatusCode = 0;
        boolean wasReset = false;
        String dbName = "patient";
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();

        // attempt to reset - returns true if successful
        wasReset = DatabaseSetup.reset_db(dbName);
        // update reset status if reset was successful
        if (wasReset == true) {
            resetStatusCode = 1;
            DatabaseSetup.createDB(dbName);
        }

        responseMap.put("reset_status_code", String.valueOf(resetStatusCode));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // Real-time Reporting Functions
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        ArrayList<String> zipList = new ArrayList<String>();
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();
        String dbname = "patient";
        String login = "root";
        String password = "rootpwd";
        String onAlert = "Y";
        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet zipsOnAlert = db.query("SELECT * FROM ZipDetails WHERE onAlert = ?", onAlert);
            while(zipsOnAlert.hasNext()){
                OResult row = zipsOnAlert.next();
                String zip = row.getProperty("zip");
                zipList.add(zip);
            }
        }
        catch (Exception e){
            System.out.println(e);
        }

        orientdb.close();
        responseMap.put("ziplist", zipList);
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();

    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        int statusState = 0;
        int alertCount = 0;
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();
        String dbname = "patient";
        String login = "root";
        String password = "rootpwd";
        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet zipsOnAlert = db.query("SELECT onAlert FROM ZipDetails");
            while(zipsOnAlert.hasNext()){
                OResult isStateOnAlert = zipsOnAlert.next();
                String onAlert = isStateOnAlert.getProperty("onAlert");
                System.out.println("onAlert = " + onAlert);
                if(onAlert.equals("Y")){
                    alertCount++;
                    System.out.println("alertCount = " + alertCount);
                }
            }
            if(alertCount >= 5){
                statusState = 1;
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
        orientdb.close();
        responseMap.put("state_status", String.valueOf(statusState));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/testcount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response testCount(@HeaderParam("X-Auth-API-Key") String authKey) {
        int numPositive = 0;
        int numNegative = 0;
        String dbname = "patient";
        String login = "root";
        String password = "rootpwd";

        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();

        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet code1 = db.query("SELECT * FROM Patient WHERE statusCode = 1"); // tested negative
            OResultSet code2 = db.query("SELECT * FROM Patient WHERE statusCode = 2"); // tested positive
            OResultSet code4 = db.query("SELECT * FROM Patient WHERE statusCode = 4"); // tested negative
            OResultSet code5 = db.query("SELECT * FROM Patient WHERE statusCode = 5"); // tested positive
            OResultSet code6 = db.query("SELECT * FROM Patient WHERE statusCode = 6"); // tested positive

            long s1 = code1.stream().count();
            long s2 = code2.stream().count();
            long s4 = code4.stream().count();
            long s5 = code5.stream().count();
            long s6 = code6.stream().count();

            long count = 0;
            long upCount = 1;
            while (count < s1) {
                numNegative = numNegative + 1;
                count = Long.sum(count, upCount);
            }
            count = 0;
            while (count < s2) {
                numPositive = numPositive + 1;
                count = Long.sum(count, upCount);
            }
            count = 0;
            while (count < s4) {
                numNegative = numNegative + 1;
                count = Long.sum(count, upCount);
            }
            count = 0;
            while (count < s5) {
                numPositive = numPositive + 1;
                count = Long.sum(count, upCount);
            }
            count = 0;
            while (count < s6) {
                numPositive = numPositive + 1;
                count = Long.sum(count, upCount);
            }

        } catch (Exception e) {
            System.out.println(e);
        }

        orientdb.close();
        responseMap.put("positive_test", String.valueOf(numPositive));
        responseMap.put("negative_test", String.valueOf(numNegative));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // Logic and Operational Functions
    @GET
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatient(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("mrn") String mrn) {
        String locationCode = "-1";
        String dbname = "patient";
        String login = "root";
        String password = "rootpwd";
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();

        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet hospitalId = db.query("SELECT hospitalId FROM Patient WHERE mrn = ?", mrn);

            while (hospitalId.hasNext()) {
                OResult row = hospitalId.next();
                locationCode = row.getProperty("hospitalId");
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        orientdb.close();
        responseMap.put("mrn", mrn);
        responseMap.put("location_code", locationCode);
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/gethospital/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHospital(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("id") String id) {
        String dbname = "patient";
        String login = "root";
        String password = "rootpwd";
        String responseString = "{}";
        int numTotalBeds = 0;
        String hospitalZipCode = "";
        int availableBeds = 0;
        int numBedsTaken = 0;
        Map<String, Object> responseMap = new HashMap<>();

        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet totalBeds = db.query("SELECT totalBeds FROM Hospital WHERE id = ?", id);
            OResultSet zipCode = db.query("SELECT zip FROM Hospital WHERE id =  ?", id);

            if (totalBeds.hasNext()) {
                OResult row = totalBeds.next();
                numTotalBeds = Integer.parseInt(row.getProperty("totalBeds"));
            }
            if (zipCode.hasNext()) {
                OResult row2 = zipCode.next();
                hospitalZipCode = row2.getProperty("zip");
            }

            OResultSet patientsAtHospital = db.query("SELECT * FROM Patient WHERE hospitalId = ?", id);
            while (patientsAtHospital.hasNext()) {
                OResult row3 = patientsAtHospital.next();
                numBedsTaken = numBedsTaken + 1;
            }

            availableBeds = numTotalBeds - numBedsTaken;
            db.command("UPDATE Hospital SET availableBeds = '?' WHERE id LIKE '?' ", availableBeds, id);

            responseMap.put("total_beds", String.valueOf(numTotalBeds));
            responseMap.put("avalable_beds", String.valueOf(availableBeds));
            responseMap.put("zipcode", hospitalZipCode);
        } catch (Exception e) {
            System.out.println(e);
        }
        orientdb.close();
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

}
