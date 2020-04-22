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

import cs505pubsubcep.Launcher;
import cs505pubsubcep.CEP.accessRecord;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    //check local
    //curl --header "X-Auth-API-key:1234" "http://localhost:8082/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8082/api/checkmycep"
    //curl --header "X-Auth-API-key:1234" "http://localhost:8081/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8081/api/checkmycep"

    @GET
    @Path("/checkmycep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkMyEndpoint(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            //get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            Map<String,String> responseMap = new HashMap<>();
            if(Launcher.cepEngine != null) {

                    responseMap.put("success", Boolean.TRUE.toString());
                    responseMap.put("status_desc","CEP Engine exists");

            } else {
                responseMap.put("success", Boolean.FALSE.toString());
                responseMap.put("status_desc","CEP Engine is null!");
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

            //get remote ip address from request
            String remoteIP = request.get().getRemoteAddr();
            //get the timestamp of the request
            long access_ts = System.currentTimeMillis();
            System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

            //generate event based on access
            String inputEvent = gson.toJson(new accessRecord(remoteIP,access_ts));
            System.out.println("inputEvent: " + inputEvent);

            //send input event to CEP
            Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent);

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("accesscoint",String.valueOf(Launcher.accessCount));
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
        String sarahId = "12145986";
        String amberlynId = "12062818";
        String[] teamMemberSids = {"12145986", "12062818"};
        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();

        int appStatusCode = 0;
        // if(){ // if app is online
        //     appStatusCode = 1;
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
        Map<String,Object> responseMap = new HashMap<>();
        // attempt to reset 

        // update reset status if reset was successful

        responseMap.put("reset_status_code", String.valueOf(resetStatusCode));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    //Real-time Reporting Functions
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipAlertList(@HeaderParam("X-Auth-API-Key") String alertStatus) {
        String[] zipList = {};
        boolean alertState = false; // growth of 2X over a 15 second time interval then true
        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();
        
        responseMap.put("ziplist", zipList);
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        int statusState = 0;

        // if(){ // state is in alert
        //     statusState = 1;
        // }

        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();
        
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

        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();
        
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
        int totalBeds = 0;
        int availableBeds = 0;
        String zipCode = "";

        String responseString = "{}";
        Map<String,Object> responseMap = new HashMap<>();
        
        responseMap.put("total_beds", String.valueOf(totalBeds));
        responseMap.put("avalable_beds", String.valueOf(availableBeds));
        responseMap.put("zipcode", zipCode);
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

}
