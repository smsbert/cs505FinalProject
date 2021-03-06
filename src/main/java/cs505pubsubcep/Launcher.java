package cs505pubsubcep;

import cs505pubsubcep.CEP.CEPEngine;
import cs505pubsubcep.Topics.TopicConnector;
import cs505pubsubcep.httpfilters.AuthenticationFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;

// java -jar target/cs505-pubsub-cep-template-1.0-SNAPSHOT.jar

public class Launcher {

    // public static final String API_SERVICE_KEY = "12062818"; //Amberlyn's
    public static final String API_SERVICE_KEY = "12145986"; // Change this to your student id
    public static final int WEB_PORT = 8088;
    public static String inputStreamName = null;
    public static long accessCount = -1;

    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;

    public static void main(String[] args) throws IOException {

        DatabaseSetup.resetZipDB("Zip");
        DatabaseSetup.createZipDB("Zip");
        String dbName = "patient";
        boolean wasReset = DatabaseSetup.reset_db(dbName);
        if (wasReset == true) {
            DatabaseSetup.createDB(dbName);
        }

        System.out.println("Starting CEP...");
        // Embedded database initialization

        cepEngine = new CEPEngine();

        // START MODIFY
        inputStreamName = "PatientInStream";
        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";

        String outputStreamName = "PatientOutStream";
        // String outputStreamAttributesString = "patient_status_code string, count long";
        String outputStreamAttributesString = "zip_code string, patient_status_code string";

        // String queryString = " " +
        // "from PatientInStream#window.timeBatch(5 sec) " +
        // "select patient_status_code, count() as count " +
        // "group by patient_status_code " +
        // "insert into PatientOutStream; ";

        String queryString = " " +
        "from PatientInStream#window.timeBatch(15 sec) " +
        "select zip_code, patient_status_code " +
        "insert into PatientOutStream; ";
        // END MODIFY

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString,
                outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");

        // starting Collector
        topicConnector = new TopicConnector();
        topicConnector.connect();

        // Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig().packages("cs505pubsubcep.httpcontrollers")
                .register(AuthenticationFilter.class);

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
