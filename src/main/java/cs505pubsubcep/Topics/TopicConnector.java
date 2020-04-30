package cs505pubsubcep.Topics;

import java.io.File;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import cs505pubsubcep.Launcher;

public class TopicConnector {

    private Gson gson;

    final Type typeOf = new TypeToken<List<Map<String, String>>>() {
    }.getType();

    private String EXCHANGE_NAME = "patient_data";

    public TopicConnector() {
        gson = new Gson();
    }

    public void connect() {

        try {

            String username = "student";
            String password = "student01";
            String hostname = "128.163.202.61";
            String virtualhost = "patient_feed";

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(hostname);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualhost);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, EXCHANGE_NAME, "#");

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(
                        " [x] Received Batch'" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                List<Map<String, String>> incomingList = gson.fromJson(message, typeOf);
                // connect database
                OrientDB orientdb = new OrientDB("remote:smsb222.cs.uky.edu", OrientDBConfig.defaultConfig());

                File file = new File("src/main/java/cs505pubsubcep/patients.csv");
                file.createNewFile();
                // open database session
                try (ODatabaseSession db = orientdb.open("patient", "root", "rootpwd");) {
                    for (Map<String, String> map : incomingList) {
                        System.out.println("INPUT CEP EVENT: " + map);

                        String firstName = map.get("first_name");
                        String lastName = map.get("last_name");
                        String mrn = map.get("mrn");
                        String zipcode = map.get("zip_code");
                        String statusCode = map.get("patient_status_code");
                        OVertex patient = db.newVertex("Patient");
                        patient.setProperty("firstName", firstName);
                        patient.setProperty("lastName", lastName);
                        patient.setProperty("mrn", mrn);
                        patient.setProperty("zipcode", zipcode);
                        patient.setProperty("statusCode", statusCode);
                        patient.save();
                        determineHospital(patient, zipcode, statusCode, db);
                        patient.save();
                        Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
                    }
                    System.out.println(
                            " [x] Received Batch'" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                } catch (Exception e) {
                    System.out.println(e);
                } 
                System.out.println("");
                System.out.println("");
                orientdb.close();
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void determineHospital(OVertex patient, String zipcode, String statusCode, ODatabaseSession db) {
        Double shortestDistance = 100000000.0000;
        String shortestZip = "";
        String locationId = "";
        if (statusCode.equals("0") || statusCode.equals("1") || statusCode.equals("2") || statusCode.equals("4")) {
            patient.setProperty("hospitalId", "0"); // Meaning patient was assigned to stay home
        } else if (statusCode.equals("3") || statusCode.equals("5")) {
            OResultSet distances = db.query(
                    "SELECT * FROM ZipDistance WHERE zipFrom = ? AND zipTo IN (SELECT zip FROM Hospital WHERE NOT availableBeds = '0')",
                    zipcode);
            // compare distance
            while (distances.hasNext()) {
                OResult row = distances.next();
                String zipTo = row.getProperty("zipTo");
                Double distance = Double.parseDouble(row.getProperty("distance"));
                if (distance.compareTo(shortestDistance) < 0) {
                    shortestDistance = distance;
                    shortestZip = zipTo;
                }
            }

            // get hospital id for location id
            OResultSet id = db.query("SELECT id FROM Hospital WHERE zip = ?", shortestZip);
            if (id.hasNext()) {
                OResult row2 = id.next();
                locationId = row2.getProperty("id");
            }

            // set hospital id to that of the shortest distance
            patient.setProperty("hospitalId", locationId); // Meaning patient was assignedclosetest facility
        } else if (statusCode.equals("6")) {
            OResultSet hospitals = db.query(
                    "SELECT * FROM ZipDistance WHERE zipFrom = ? AND zipTo IN (SELECT zip FROM Hospital WHERE trauma <> 'NOT AVAILABLE')",
                    zipcode);
            while (hospitals.hasNext()) {
                OResult row = hospitals.next();
                String zipTo = row.getProperty("zipTo");
                Double distance = Double.parseDouble(row.getProperty("distance"));
                if (distance.compareTo(shortestDistance) < 0) {
                    shortestDistance = distance;
                    shortestZip = zipTo;
                }
            }
            // get hospital id for location id
            OResultSet id = db.query("SELECT id FROM Hospital WHERE zip = ?", shortestZip);
            if (id.hasNext()) {
                OResult row2 = id.next();
                locationId = row2.getProperty("id");
            }
            patient.setProperty("hospitalId", locationId); // Meaning patient was assigned the closest available Level
                                                           // IV (I > II > III > IV) or better treatment facility
        } else {
            patient.setProperty("hospitalId", "-1"); // Meaning patient was not assigned anything
        }
        patient.save();
    }
}
