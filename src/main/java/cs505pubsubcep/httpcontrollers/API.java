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

    // TODO: implement all functions, right now they return 400 status until implemeneted
    // Application Management Functions 
    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeam(@HeaderParam("X-Auth-API-Key") String authKey) {
        String teamName = "404 Team Name Not Found";
        int[] teamMemberSids = {12145986, 12062818};
        int appStatusCode = 0;
        // if(){ // if app is online
        //     appStatusCode = 1;
        // }
        return Response.ok().header("team_name", teamName)
            .header("Team_members_sids", teamMemberSids)
            .header("app_status_code", appStatusCode).build();
        // return Response.status(400).build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset(@HeaderParam("X-Auth-API-Key") String authKey) {
        int resetStatusCode = 0;
        boolean wasReset = false;
        // attempt to reset 

        // update reset status
        // if()
        return Response.ok().header("reset_status_code", resetStatusCode).build();
        // return Response.status(400).build();
    }


    //Real-time Reporting Functions
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
       
        return Response.status(400).build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        
        return Response.status(400).build();
    }
    
    @GET
    @Path("/testcount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response testCount(@HeaderParam("X-Auth-API-Key") String authKey) {
     
        return Response.status(400).build();
    }


    // Logic and Operational Functions
    @GET
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatient(@HeaderParam("X-Auth-API-Key") String authKey) {
      
        return Response.status(400).build();
    }

    @GET
    @Path("/gethospital/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHospital(@HeaderParam("X-Auth-API-Key") String authKey) {
       
        return Response.status(400).build();
    }

}
