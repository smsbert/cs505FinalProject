package cs505pubsubcep.CEP;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mchange.v2.c3p0.impl.DbAuth;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.serializer.result.binary.OResultSerializerNetwork;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.lang.reflect.Type;

import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    private Gson gson;

    public static int count = 0;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    // final Type typeOf = new TypeToken<List<Map<String, String>>>() {
    // }.getType();
    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            BufferedWriter patientWriter = new BufferedWriter(
                    new FileWriter("src/main/java/cs505pubsubcep/patients.csv", true) // Set true for append
                                                                                      // mode
            );
            // String msgString = msg.toString();
            // List<Map<String, String>> incomingList = gson.fromJson(msgString, typeOf);
            // for (Map<String, String> map : incomingList) {

            String mapString = msg.toString();
            // String mapString = map.toString();
            patientWriter.newLine(); // Add new line
            patientWriter.write(mapString);
            patientWriter.close();

            String timeInterval = "t" + count;
            OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
            ArrayList<String> distinctZipCodes = new ArrayList<String>();
            try (ODatabaseSession zipDb = orientdb.open("Zip", "root", "rootpwd");) {

                try (BufferedReader br = new BufferedReader(
                        new FileReader("src/main/java/cs505pubsubcep/patients.csv"))) {
                    for (String line; (line = br.readLine()) != null;) {
                        // String line2 = line.replaceAll("\\[", "");
                        // line.replaceAll("\\]", "");
                        // line.replaceAll("\\{", "");
                        // line.replaceAll("\\}", "");
                        // line.replaceAll("\"", "");
                        // line.replaceAll("event", "");
                        // line.replaceAll("\\:", "");
                        // System.out.println("LINE = " + line);
                        String[] vertices = line.split("event");
                        for (String v : vertices) {
                            System.out.println("VERTICES = " + v);
                            if (v.length() >= 46) {
                                String zipcode = v.substring(15, 20);
                                String patientStatus = v.substring(45, 46);
                                System.out.println("ZIPCODE = " + zipcode);
                                System.out.println("PATIENTSTATUS = " + patientStatus);

                                if (distinctZipCodes.contains(zipcode) == false) {
                                    distinctZipCodes.add(zipcode);
                                }

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
                int numPos = 0;
                for (String zipCode : distinctZipCodes) {
                    OResultSet numberPositive = zipDb.query(
                            "SELECT * FROM Zip WHERE statusCode = 2 OR statusCode = 5 OR statusCode = 6 AND zip = ? and timeInterval = ?",
                            zipCode, timeInterval);
                    while (numberPositive.hasNext()) {
                        numPos++;
                    }
                    System.out.print("NUMPOS = " + numPos);
                    OVertex newNumPatient = zipDb.newVertex("NumPatients");
                    newNumPatient.setProperty("zipcode", zipCode);
                    newNumPatient.setProperty("timeInterval", timeInterval);
                    newNumPatient.setProperty("numPatients", String.valueOf(numPos));
                    newNumPatient.save();
                    if(count != 0){
                        String prevTimeInterval = "t" + (count - 1);
                        OResultSet currentNumPos = zipDb.query("SELECT * FROM NumPatients WHERE zipcode = ? AND timeInterval = ?", zipCode, timeInterval); 
                        OResultSet prevNumPos = zipDb.query("SELECT * FROM NumPatients WHERE zipcode = ? AND timeInterval = ?", zipCode, prevTimeInterval); 

                        int currentPos = Integer.parseInt(currentNumPos.next().getProperty("numPatients"));
                        int prevPos = Integer.parseInt(prevNumPos.next().getProperty("numPatients"));
                        int growth = 2 * prevPos;

                        if(currentPos > growth){
                            System.out.println("GROWTH WAS DOUBLED");
                            newNumPatient.setProperty("alertStatus", "Y");
                            newNumPatient.save();
                        }
                        else {
                            System.out.println("NO GROWTH");
                            newNumPatient.setProperty("alertStatus", "N");
                            newNumPatient.save();
                        }
                    }
                    int numZipsInAlert = 0;
                    OResultSet totalZipsInAlert = zipDb.query("SELECT * FROM NumPatients WHERE alertStatus = Y AND timeInterval = ?", timeInterval);
                    while(totalZipsInAlert.hasNext()){
                        OResult nextZip = totalZipsInAlert.next();
                        numZipsInAlert++;
                    }
                    System.out.println("NUMZIPSINALERT = " + numZipsInAlert);
                    if(numZipsInAlert > 5){
                        zipDb.command("UPDATE StateAlert SET onAlert = Y");
                        System.out.println("ALERT SHOULD BE Y");
                    }
                    else{
                        zipDb.command("UPDATE StateAlert SET onAlert = N");
                        System.out.println("ALERT SHOULD BE N");
                    }
                }

                orientdb.close();
            } catch (Exception e) {
                System.out.println(e);
            }
            // String[] sstr = String.valueOf(msg).split(":");
            // String[] outval = sstr[2].split("}");
            // Launcher.accessCount = Long.parseLong(outval[0]);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        count++;
    }

    @Override
    public String getTopic() {
        return topic;
    }

}
