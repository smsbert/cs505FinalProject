package cs505pubsubcep.httpcontrollers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

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
    // curl --header "X-Auth-API-key:1234" "http://localhost:8082/api/checkmycep"

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

    // TODO: implement all functions
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
        // if(){ // if app is online
        // appStatusCode = 1;
        // }

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
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();

        // attempt to reset - returns true if successful
        wasReset = DatabaseSetup.reset_db("filename");
        // update reset status if reset was successful
        if (wasReset == true) {
            resetStatusCode = 1;
        }

        responseMap.put("reset_status_code", String.valueOf(resetStatusCode));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // Real-time Reporting Functions
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipAlertList(@HeaderParam("X-Auth-API-Key") String alertStatus) {
        String[] zipList = {};
        boolean alertState = false; // growth of 2X over a 15 second time interval then true
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();

        alertZipList = zipList;
        responseMap.put("ziplist", zipList);
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        int statusState = 0;
        String responseString = "{}";
        Map<String, Object> responseMap = new HashMap<>();

        if (alertZipList.length >= 5) { // state is in alert if at least five zipcodes are in alert
            statusState = 1;
        }

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
        Map<String,Object> responseMap = new HashMap<>();

        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet code1 = db.query("SELECT COUNT(*) FROM Patient WHERE statusCode = 1");
            OResultSet code2 = db.query("SELECT COUNT(*) FROM Patient WHERE statusCode = 2");
            OResultSet code3 = db.query("SELECT COUNT(*) FROM Patient WHERE statusCode = 3");
            OResultSet code4 = db.query("SELECT COUNT(*) FROM Patient WHERE statusCode = 4");
            OResultSet code5 = db.query("SELECT COUNT(*) FROM Patient WHERE statusCode = 5");
            OResultSet code6 = db.query("SELECT COUNT(*) FROM Patient WHERE statusCode = 6");
            // numPositive = code2. + code5 + code6;
            // numNegative = code1 + code4;

        }
        catch (Exception e){
            System.out.println(e);
        }
        
        responseMap.put("positive_test", String.valueOf(numPositive));
        responseMap.put("negative_test", String.valueOf(numNegative));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    // Logic and Operational Functions
    @GET
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatient(@HeaderParam("X-Auth-API-Key") String authKey) {
        String mrn = "";
        String locationCode = "";
        int homeAssignment = 0;
        int noAssignment = -1;


        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();
        
        responseMap.put("mrn", mrn);
        responseMap.put("location_code", locationCode);
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/gethospital/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHospital(@HeaderParam("X-Auth-API-Key") String authKey) {
        String dbname = "patient";
        String login = "root";
        String password = "rootpwd";
        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();
        String id = ""; //get id from path

        OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            OResultSet totalBeds = db.query("SELECT beds FROM Hospital WHERE id = ", id);
            OResultSet zipCode = db.query("SELECT zip FROM Hospital WHERE id =  ", id);
            String availableBeds = ""; // update based on patients status code 3, 5, 6 where they need a bed
        // get patients with status 3, 5, 6 that are at that hospital to reduce bed number 
            responseMap.put("total_beds", String.valueOf(totalBeds));
            responseMap.put("avalable_beds", String.valueOf(availableBeds));
            responseMap.put("zipcode", zipCode);
        }
        catch (Exception e){
            System.out.println(e);
        }
        
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

}