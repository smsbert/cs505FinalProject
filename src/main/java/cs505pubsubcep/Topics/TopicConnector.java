package cs505pubsubcep.Topics;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import org.antlr.v4.runtime.atn.SemanticContext.OR;

import cs505pubsubcep.Launcher;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Stream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

public class TopicConnector {

    private Gson gson;
    public static int count;
    public static boolean checkStatus;

    final Type typeOf = new TypeToken<List<Map<String, String>>>() {
    }.getType();

    private String EXCHANGE_NAME = "patient_data";

    public TopicConnector() {
        gson = new Gson();
    }

    public void connect() {

        try {

            // String hostname = "";
            // String username = "";
            // String password = "";
            // String virtualhost = "";
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
            count = 0;
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(
                        " [x] Received Batch'" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                List<Map<String, String>> incomingList = gson.fromJson(message, typeOf);
                // connect database
                OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
                // OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
                // poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 1);
                // poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10);
                // ODatabasePool pool = new ODatabasePool(orientdb, "patient", "root",
                // "rootpwd", poolCfg.build());

		File file = new File("src/main/java/cs505pubsubcep/patients.csv");
            	file.createNewFile();
		//open database session
                try (ODatabaseSession db = orientdb.open("patient", "root", "rootpwd");) {
                    for (Map<String, String> map : incomingList) {
                        System.out.println("INPUT CEP EVENT: " + map);
                        
			BufferedWriter patientWriter = new BufferedWriter(
                                new FileWriter("src/main/java/cs505pubsubcep/patients.csv", true)  //Set true for append mode
                            );
                        String mapString = map.toString();
                        patientWriter.newLine();   //Add new line
                        patientWriter.write(mapString);
                        patientWriter.close();
			
			String firstName = map.get("first_name");
                        String lastName = map.get("last_name");
                        String mrn = map.get("mrn");
                        String zipcode = map.get("zip_code");
                        String statusCode = map.get("patient_status_code");
                        OVertex patient = db.newVertex("Patient");
                        Date dateTime = new Date();
                        if (checkStatus == true) {
                            int numNewPatients = 0;
                            // try (ODatabaseSession db2 = orientdb.open("patient", "root", "rootpwd")) {
                            if (count != 0) {
                                String timeInterval = "t" + count;
                                String onAlert = "Y";
                                String offAlert = "N";
                                int numZipsOnalert = 0;
                                OResultSet patients = db.query("SELECT * FROM Patient WHERE timeInterval = ? AND statusCode = '2' OR statusCode = '5' OR statusCode = '6'",
                                        timeInterval);
                                System.out.println(timeInterval);
                                while (patients.hasNext()) {
                                    OResult p = patients.next();
                                    String zip = p.getProperty("zipcode");
                                    OResultSet zipdetails = db.query("SELECT * FROM ZipDetails WHERE zip = ?",
                                            zip);

                                    String numPatients = zipdetails.next().getProperty("numPatients");
                                    String unUpdatedPatients = zipdetails.next().getProperty("patients");
                                    int prevTimeNumPatient = Integer.parseInt(unUpdatedPatients);
                                    int growthRateAlertPatients = 2 * prevTimeNumPatient;

                                    int numOldPatients = Integer.parseInt(numPatients);
                                    numNewPatients = numOldPatients + 1;
                                    db.command("UPDATE ZipDetails SET numPatients = ? WHERE zip  = ?", numNewPatients,
                                            zip);
                                    System.out.println("UPDATE numPatients");

                                    if (numNewPatients >= growthRateAlertPatients) {
                                        db.command("UPDATE ZipDetails SET onAlert = ? WHERE zip = ?", onAlert, zip);
                                        System.out.println("UPDATE onalert");

                                        numZipsOnalert++;
                                    } else {
                                        db.command("UPDATE ZipDetails SET onAlert = ? WHERE zip = ?", offAlert, zip);
                                        System.out.println("UPDATE offalert");

                                    }
                                }
                                if (numZipsOnalert >= 5) {
                                    db.command("UPDATE Patient SET stateAlert = ?", onAlert);
                                    System.out.println("UPDATE statealert");

                                } else {
                                    db.command("UPDATE Patient SET stateAlert = ?", onAlert);

                                    System.out.println("UPDATE statealertoff");
                                }
                            }
                            // pool.close();
                            // orientdb2.close();
                            // }catch(Exception e){
                            // System.out.println(e);
                            // } finally {
                            // orientdb.close();
                            // }
                            checkStatus = false;
                        }
                        Timer timer = new Timer();
                        timer.schedule(new TimerTask() {

                            @Override
                            public void run() {
                                // TODO Auto-generated method stub
                                System.out.println("15 seconds passed!");
                                count++;
                                checkStatus = true;
                            }
                        }

                                , 15000);
                        String timeInterval = "t" + count;
                        patient.setProperty("dateTime", dateTime);
                        patient.setProperty("timeInterval", timeInterval);
                        patient.setProperty("firstName", firstName);
                        patient.setProperty("lastName", lastName);
                        patient.setProperty("mrn", mrn);
                        patient.setProperty("zipcode", zipcode);
                        patient.setProperty("statusCode", statusCode);
                        patient.save();
                        List<HashMap<String, String>> listForCsv = new ArrayList<HashMap<String, String>>();
                        HashMap<String, String> csvMap;
                        csvMap = new HashMap<String, String>();
                        csvMap.put("timeInterval", timeInterval);
                        csvMap.put("zipcode", zipcode);
                        csvMap.put("onAlert", "N");
                        listForCsv.add(csvMap);
                        // updateZipDb(listForCsv);

                        determineHospital(patient, zipcode, statusCode, db);
                        Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
                    }
                    System.out.println(
                            " [x] Received Batch'" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                } catch (Exception e) {
                    System.out.println(e);
                } finally {
                    orientdb.close();
                }
                System.out.println("");
                System.out.println("");
                // pool.close();
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
        System.out.println("STATUSCODE = " + statusCode);
        if (statusCode.equals("0") || statusCode.equals("1") || statusCode.equals("2") || statusCode.equals("4")) {
            patient.setProperty("hospitalId", "0"); // Meaning patient was assigned to stay home
        } else if (statusCode.equals("3") || statusCode.equals("5")) {
            OResultSet distances = db.query(
                    "SELECT * FROM ZipDistance WHERE zipFrom = ? AND zipTo IN (SELECT zip FROM Hospital WHERE NOT availableBeds = '0')",
                    zipcode);
            // compare distance
            System.out.println("LOOK HERE");
            // System.out.println("zipFrom = " + zipFrom + " zipTo = " + zipTo + " distance
            // = " + distance);
            while (distances.hasNext()) {
                OResult row = distances.next();
                String zipTo = row.getProperty("zipTo");
                Double distance = Double.parseDouble(row.getProperty("distance"));
                if (distance.compareTo(shortestDistance) < 0) {
                    shortestDistance = distance;
                    shortestZip = zipTo;
                    System.out.println("shortestZip = " + shortestZip + " zipcode = " + zipcode);
                    System.out.println("Distance = " + shortestDistance);
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
