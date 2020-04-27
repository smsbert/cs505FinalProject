package cs505pubsubcep;

import java.io.IOException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

// docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:2.2

public class DatabaseSetup {

    private static OrientDB orientdb;
    private static OrientGraph graphDB;
    private ODatabaseSession dbSession;
    private static String login = "root";
    private static String password = "rootpwd";

    public static boolean reset_db(String name) {
        boolean wasSuccessful = false;
        orientdb = new OrientDB("remote:smsb222.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());

        // Remove Old Database
        if (orientdb.exists(name)) {
            orientdb.drop(name);
        }

        orientdb.create(name, ODatabaseType.PLOCAL);
        try (ODatabaseSession db = orientdb.open(name, login, password);) {
            wasSuccessful = true;
        }
        catch (Exception e) {
            wasSuccessful = false;
            System.out.println(e);
        }
        
        orientdb.close();
        // if successful
        return wasSuccessful;
    }

    public static void main(String[] args) throws IOException {
        String dbName = "patient";
        boolean wasReset = reset_db(dbName);
        if(wasReset == true){
            createDB(dbName);
        }
    }

    public static OClass createHospitalClass(ODatabaseSession db, OClass hospital) {
        hospital = db.createVertexClass("Hospital");
        hospital.createProperty("id", OType.STRING);
        hospital.createProperty("name", OType.STRING);
        hospital.createProperty("address", OType.STRING);
        hospital.createProperty("city", OType.STRING);
        hospital.createProperty("type", OType.STRING);
        hospital.createProperty("beds", OType.STRING);
        hospital.createProperty("county", OType.STRING);
        hospital.createProperty("country", OType.STRING);
        hospital.createProperty("lattitude", OType.STRING);
        hospital.createProperty("longitude", OType.STRING);
        hospital.createProperty("naics", OType.STRING);
        hospital.createProperty("website", OType.STRING);
        hospital.createProperty("owner", OType.STRING);
        hospital.createProperty("trauma", OType.STRING);
        hospital.createProperty("heipad", OType.STRING);
        
	//bring in the csv
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/java/cs505pubsubcep/hospitals.csv"))) {
            int i=0;
            for (String line; (line = br.readLine()) !=null ; ) {
                //disregardes the first line
                if (i==0)
                {
                    i++;
                }
                else
                {
                    String[] vertices = line.split(",");

                    String id = vertices[0];
                    String name = vertices[1];
                    String address = vertices[2];
                    String city = vertices[3];
                    String type = vertices[4];
                    String beds = vertices[5];
                    String county = vertices[6];
                    String country = vertices[7];
                    String latitude = vertices[8];
                    String longitude = vertices[9];
                    String naics = vertices[10];
                    String website = vertices[11];
                    String owner = vertices[12];
                    String trauma = vertices[13];
                    String heipad = vertices[14];
                    OVertex hospitalVertex = db.newVertex("Hospital");

                    hospitalVertex.setProperty("id", id);
                    hospitalVertex.setProperty("name", name);
                    hospitalVertex.setProperty("address", address);
                    hospitalVertex.setProperty("city", city);
                    hospitalVertex.setProperty("type", type);
                    hospitalVertex.setProperty("beds", beds);
                    hospitalVertex.setProperty("county", county);
                    hospitalVertex.setProperty("country", country);
                    hospitalVertex.setProperty("latitude", latitude);
                    hospitalVertex.setProperty("longitude", longitude);
                    hospitalVertex.setProperty("naics", naics);
                    hospitalVertex.setProperty("website", website);
                    hospitalVertex.setProperty("owner", owner);
                    hospitalVertex.setProperty("trauma", trauma);
                    hospitalVertex.setProperty("heipad", heipad);
                    
		    hospitalVertex.save();
		}
	}
	}
	catch (IOException e) {
        	e.printStackTrace();
    }
	
	return hospital;
    }

    public static OClass createKYZipDetailsClass(ODatabaseSession db, OClass zipDetails) {
        zipDetails = db.createVertexClass("ZipDetails");
        zipDetails.createProperty("zip", OType.STRING);
        zipDetails.createProperty("zipcodeName", OType.STRING);
        zipDetails.createProperty("city", OType.STRING);
        zipDetails.createProperty("state", OType.STRING);
        zipDetails.createProperty("countyName", OType.STRING);
        
	//bring in the csv
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/java/cs505pubsubcep/kyzipdetails.csv"))) {
            int i=0;
            for (String line; (line = br.readLine()) !=null ; ) {
                //disregardes the first line
                if (i==0)
                {
                    i++;
                }
                else
                {
                    String[] vertices = line.split(",");

                    String zip = vertices[0];
                    String zipcodeName = vertices[1];
                    String city = vertices[2];
                    String state = vertices[3];
                    String countyName = vertices[4];
                    
		    OVertex zipDetailsVertex = db.newVertex("ZipDetails");

                    zipDetailsVertex.setProperty("zip", zip);
                    zipDetailsVertex.setProperty("zipcodeName", zipcodeName);
                    zipDetailsVertex.setProperty("city", city);
                    zipDetailsVertex.setProperty("state", state);
                    zipDetailsVertex.setProperty("countyName", countyName);

		    zipDetailsVertex.save();
                }
        }
        }
        catch (IOException e) {
                e.printStackTrace();
    }	
	return zipDetails;

    }

    public static OClass createKYZipDistanceClass(ODatabaseSession db, OClass zipDistance) {
        zipDistance = db.createVertexClass("ZipDistance");
        zipDistance.createProperty("zipFrom", OType.STRING);
        zipDistance.createProperty("zipTo", OType.STRING);
        zipDistance.createProperty("distance", OType.STRING);
        
	//bring in the csv
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/java/cs505pubsubcep/kyzipdistance.csv"))) {
            int i=0;
            for (String line; (line = br.readLine()) !=null ; ) {
                //disregardes the first line
                if (i==0)
                {
                    i++;
                }
                else
                {
                    String[] vertices = line.split(",");

                    String zipFrom = vertices[0];
                    String zipTo = vertices[1];
                    String distance = vertices[2];
                    
                    OVertex zipDistanceVertex = db.newVertex("ZipDistance");

                    zipDistanceVertex.setProperty("zipFrom", zipFrom);
                    zipDistanceVertex.setProperty("zipTo", zipTo);
                    zipDistanceVertex.setProperty("distance", distance);

                    zipDistanceVertex.save();
                }
        }
        }
        catch (IOException e) {
                e.printStackTrace();
    }	
	return zipDistance;
    }

    public static OClass createPatientClass(ODatabaseSession db, OClass patient) {
        patient = db.createVertexClass("Patient");
        patient.createProperty("firstName", OType.STRING);
        patient.createProperty("lastName", OType.STRING);
        patient.createProperty("mrn", OType.STRING);
        patient.createProperty("zipcode", OType.STRING);
        patient.createProperty("statusCode", OType.STRING);
        patient.createProperty("dateTime", OType.DATETIME);
        return patient;
    }

    public void updateHospitalDB(String filepath) {
        String dbname = "patient";
        int updatedBedCount = 0;
        String hospitalId = "";

        OrientGraph graphDB = new OrientGraph("smsb222.cs.uky.edu:patient", login, password);
        graphDB.command(
                new OCommandSQL("UPDATE Hospital availableBeds = " + updatedBedCount + "WHERE id = " + hospitalId))
                .execute();
    }

    public static void createDB(String dbname) {

        // connect database
        orientdb = new OrientDB("remote:smsb222.cs.uky.edu", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            // create databases
            OClass hospital = db.getClass("Hospital");
            OClass zipDetails = db.getClass("ZipDetails");
            OClass zipDistance = db.getClass("ZipDistance");
            OClass patient = db.getClass("Patient");
            if(patient == null){
                patient = createPatientClass(db, patient);
            }
	    if(hospital == null){
                hospital = createHospitalClass(db, hospital);
            }
            if(zipDetails == null){
                zipDetails = createKYZipDetailsClass(db, zipDetails);
            }
            if(zipDistance == null){
                zipDistance = createKYZipDistanceClass(db, zipDistance);
            }
        } catch (Exception e){
            System.out.println(e);
        }
        
        orientdb.close();
    }
}
