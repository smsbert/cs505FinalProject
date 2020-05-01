package cs505pubsubcep.CEP;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Optional;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import org.antlr.v4.runtime.atn.SemanticContext.OR;
import org.quartz.simpl.SystemPropertyInstanceIdGenerator;

import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public static ArrayList<String> zipcodesCurrentlyOnAlert = new ArrayList<String>();

    public static int count = 0;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            BufferedWriter patientWriter = new BufferedWriter(
                    new FileWriter("src/main/java/cs505pubsubcep/patients.csv", true) // Set true for append
                                                                                      // mode
            );

            // write 15 seconds of info to the csv
            String mapString = msg.toString();
            patientWriter.newLine(); // Add new line
            patientWriter.write(mapString);
            patientWriter.close();

            // count is the current time interval
            String timeInterval = "t" + count;
            // start database connection
            OrientDB orientdb = new OrientDB("remote:smsb222.cs.uky.edu", OrientDBConfig.defaultConfig());
            ArrayList<String> distinctZipCodes = new ArrayList<String>(); // array to contain all the distinct zipcodes
                                                                          // from this 15 seconds of info
            try (ODatabaseSession zipDb = orientdb.open("Zip", "root", "rootpwd");) {

                // read csv file
                try (BufferedReader br = new BufferedReader(
                        new FileReader("src/main/java/cs505pubsubcep/patients.csv"))) {
                    for (String line; (line = br.readLine()) != null;) {
                        String[] vertices = line.split("event");
                        for (String v : vertices) {
                            System.out.println("VERTICES = " + v);
                            if (v.length() >= 46) {
                                String zipcode = v.substring(15, 20);
                                String patientStatus = v.substring(45, 46);
                                System.out.println("ZIPCODE = " + zipcode);
                                System.out.println("PATIENTSTATUS = " + patientStatus);

                                if (!distinctZipCodes.contains(zipcode)) {
                                    System.out.println("ADD ZIP TO ZIPLIST");
                                    distinctZipCodes.add(zipcode);
                                }

                                // create new Zip vertex based on info from csv and current time interval
                                OVertex zipInfo = zipDb.newVertex("Zip");
                                zipInfo.setProperty("zipcode", zipcode);
                                zipInfo.setProperty("statusCode", patientStatus);
                                zipInfo.setProperty("timeInterval", timeInterval);
                                zipInfo.save();
                            }
                        }

                    }
                } catch (Exception e) {
                    System.out.println(e);
                }

                // for each distinct zip code
                zipcodesCurrentlyOnAlert.clear();
                System.out.println("Distinct zips = " + distinctZipCodes);
                for (String zipCode : distinctZipCodes) {
                    int numPos = 0;
                    // find the number of patients who tested positive
                    System.out.println("TIMEINTERVAL = " + timeInterval);
                    System.out.println("ZIPCODE = " + zipCode);
                    String query = "SELECT * FROM Zip WHERE zipcode = " + zipCode + " AND timeInterval = '"
                            + timeInterval + "'";
                    System.out.println("QUERY  = " + query);
                    OResultSet numberPositive = zipDb.query(query);
                    // System.out.println("NUMBERPOSITIVE = " + numberPositive.stream().findFirst());
                    // System.out.println("ISPRSESNT = " + numberPositive.stream().findFirst().isPresent());

                    while (numberPositive.hasNext()) {
                        System.out.println("INSIDE WHILE");
                        OResult row = numberPositive.next();
                        String statusCode = row.getProperty("statusCode");
                        if (statusCode.equals("2") || statusCode.equals("5") || statusCode.equals("6")) {
                            System.out.println("UPDATE NUM POS FOR ZIP = " + zipCode);
                            numPos++;
                        }
                    }
                    System.out.println("NUMPOS = " + numPos);
                    // create new vertex containg the number of positive test cases for that zipcode
                    // at this time interval
                    OVertex newNumPatient = zipDb.newVertex("NumPatients");
                    newNumPatient.setProperty("zipcode", zipCode);
                    newNumPatient.setProperty("timeInterval", timeInterval);
                    newNumPatient.setProperty("numPatients", String.valueOf(numPos));
                    newNumPatient.save();
                    if (count == 0) {
                        int currentNumberPositive = 0;
                        String query2 =  "SELECT * FROM NumPatients WHERE zipcode = " + zipCode + " AND timeInterval = '" + timeInterval + "'";
                        OResultSet currentNumPos = zipDb.query(query2);
                        while (currentNumPos.hasNext()) {
                            OResult pos = currentNumPos.next();
                            String property = pos.getProperty("numPatients");
                            int currentPos = Integer.parseInt(property);
                            currentNumberPositive = currentNumberPositive + currentPos;
                        }
                        System.out.println("CURRENT NUM POS IN CO = " + currentNumberPositive);
                        newNumPatient.setProperty("numPatients", String.valueOf(currentNumberPositive));
                        newNumPatient.setProperty("alertStatus", "N");
                        newNumPatient.save();
                    }
                    if (count != 0) { // if this is not the first 15 seconds
                        String prevTimeInterval = "t" + (count - 1); // previous time interval
                        String query3 = "SELECT * FROM NumPatients WHERE zipcode = " + zipCode + " AND timeInterval = '" + timeInterval + "'";
                        OResultSet currentNumPos = zipDb.query(query3);
                        String query4 = "SELECT * FROM NumPatients WHERE zipcode = " + zipCode + " AND timeInterval = '" + prevTimeInterval + "'";
                        OResultSet prevNumPos = zipDb.query(query4);

                        int currentNumberPositive = 0;
                        while (currentNumPos.hasNext()) {
                            OResult pos = currentNumPos.next();
                            String row1 = pos.getProperty("numPatients");
                            int currentPos = Integer.parseInt(row1);
                            currentNumberPositive = currentNumberPositive + currentPos;
                        }
                        System.out.println("CURRENT NUM POS = " + currentNumberPositive);
                        newNumPatient.setProperty("numPatients", String.valueOf(currentNumberPositive));
                        int previousNumberPositive = 0;
                        while (prevNumPos.hasNext()) {
                            OResult pos = prevNumPos.next();
                            String row2 = pos.getProperty("numPatients");
                            int currentPos = Integer.parseInt(row2);
                            previousNumberPositive = previousNumberPositive + currentPos;

                        }
                        int growth = 2 * previousNumberPositive;

                        // growth for this zipcode has doubled
                        if (currentNumberPositive > growth) {
                            System.out.println("GROWTH WAS DOUBLED");
                            newNumPatient.setProperty("alertStatus", "Y");
                            newNumPatient.save();
                            zipcodesCurrentlyOnAlert.add(zipCode);
                        } else {
                            System.out.println("NO GROWTH");
                            newNumPatient.setProperty("alertStatus", "N");
                            newNumPatient.save();
                        }
                    }
                    // check how many zipcodes are in alert
                    int numZipsInAlert = 0;
                    String query5 = "SELECT DISTINCT(zipcode) FROM NumPatients WHERE alertStatus = 'Y' AND timeInterval = '" + timeInterval + "'";
                    OResultSet totalZipsInAlert = zipDb.query(query5);
                    while (totalZipsInAlert.hasNext()) {
                        OResult nextZip = totalZipsInAlert.next();
                        numZipsInAlert++;
                    }
                    System.out.println("NUMZIPSINALERT = " + numZipsInAlert);
                    if (numZipsInAlert >= 5) {
                        OResultSet alertState = zipDb.query("SELECT * FROM StateAlert");
                        while (alertState.hasNext()) {
                            OResult row3 = alertState.next();
                            Optional<OVertex> vertex = row3.getVertex();
                            vertex.get().setProperty("onAlert", "Y");
                        }
                        System.out.println("ALERT SHOULD BE Y");
                    } else {
                        OResultSet alertState = zipDb.query("SELECT * FROM StateAlert");
                        while (alertState.hasNext()) {
                            OResult row4 = alertState.next();
                            Optional<OVertex> vertex = row4.getVertex();
                            vertex.get().setProperty("onAlert", "N");
                        }
                        System.out.println("ALERT SHOULD BE N");
                    }
                }

                orientdb.close();
            } catch (Exception e) {
                System.out.println(e);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        count++;
    }


    public static ArrayList<String> getZipsOnAlert(){
        return zipcodesCurrentlyOnAlert;
    }


    @Override
    public String getTopic() {
        return topic;
    }

}
