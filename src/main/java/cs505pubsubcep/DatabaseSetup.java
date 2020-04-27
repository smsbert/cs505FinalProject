package cs505pubsubcep;

import java.io.IOException;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
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
        orientdb = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());

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

    // public void printJSONDB(String filepath){

    // BufferedReader reader = new BufferedReader(new FileReader (filepath));
    // JSONArray data = json.load(reader);
    // with open(filepath) as f {
    // data = json.load(f);
    // }

    // for (Object key : data) {

    // String name = data.get(key).get("name");
    // String wikiUrl = data.get(key).get("wikiUrl");
    // String wikiImage = data.get(key).get("wikiImage");
    // String degreeLists = data.get(key).get("degreeLists");

    // System.out.print("json key:\t\t" + key);
    // System.out.print("name:\t\t\t" + name);

    // // Most people are on Wikipedia, but not everyone
    // if (wikiUrl == null) {
    // System.out.print("wikiUrl:\t\t" + wikiUrl);
    // }
    // if (wikiImage == null) {
    // System.out.print("wikiImage:\t\t" + wikiImage);
    // }

    // System.out.print(degreeLists);
    // System.out.print("");
    // }
    // }

    public static OClass createHospitalClass(ODatabaseSession db, OClass hospital) {
        hospital = db.createVertexClass("Hospital");
        hospital.createProperty("id", OType.STRING);
        hospital.createProperty("name", OType.STRING);
        hospital.createProperty("address", OType.STRING);
        hospital.createProperty("city", OType.STRING);
        hospital.createProperty("type", OType.STRING);
        hospital.createProperty("beds", OType.INTEGER);
        hospital.createProperty("county", OType.STRING);
        hospital.createProperty("country", OType.STRING);
        hospital.createProperty("lattitude", OType.FLOAT);
        hospital.createProperty("longitude", OType.FLOAT);
        hospital.createProperty("naics", OType.STRING);
        hospital.createProperty("website", OType.STRING);
        hospital.createProperty("owner", OType.STRING);
        hospital.createProperty("trauma", OType.STRING);
        hospital.createProperty("heipad", OType.BOOLEAN);
        return hospital;
    }

    public static OClass createKYZipDetailsClass(ODatabaseSession db, OClass zipDetails) {
        zipDetails = db.createVertexClass("ZipDetails");
        zipDetails.createProperty("zip", OType.STRING);
        zipDetails.createProperty("zipcodeName", OType.STRING);
        zipDetails.createProperty("city", OType.STRING);
        zipDetails.createProperty("state", OType.STRING);
        zipDetails.createProperty("countyName", OType.STRING);
        return zipDetails;

    }

    public static OClass createKYZipDistanceClass(ODatabaseSession db, OClass zipDistance) {
        zipDistance = db.createVertexClass("ZipDistance");
        zipDistance.createProperty("zipFrom", OType.STRING);
        zipDistance.createProperty("zipTo", OType.STRING);
        zipDistance.createProperty("distance", OType.FLOAT);
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

        OrientGraph graphDB = new OrientGraph("localhost:patient", login, password);
        graphDB.command(
                new OCommandSQL("UPDATE Hospital availableBeds = " + updatedBedCount + "WHERE id = " + hospitalId))
                .execute();
    }

    public void addPatientsToDB(String filepath) {
        // // go thourhg file and add each patient to database
        // String dbname = "patient";
        // String login = "root";
        // String password = "rootpwd";
        // OrientGraph graphDB = new OrientGraph("localhost:patient", login, password);
        // // open and parse local json file
        // with open(filepath) as f {
        // data = json.load(f);
        // }
        // // loop through each key in the json and create a new patient with the info
        // in the database
        // for (Object key : data) {
        // String firstName = data.get(key).get("first_name");
        // String lastName = data.get(key).get("last_name");
        // String mrn = data.get(key).get("mrn");
        // String zipcode = str(data.get(key).get("zip_code"));
        // String statusCode = str(data.get(key).get("patient_status_code"));

        // graphDB.command(new OCommandSQL("CREATE VERTEX Person SET firstName = '" +
        // firstName + "', lastName = '" + lastName
        // + "', mrn = '" + mrn + "', zipcode = '" + zipcode + "', statusCode = '" +
        // statusCode + "'")).execute();
        // }
    }

    public static void createDB(String dbname) {

        // connect database
        orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        // open database session
        try (ODatabaseSession db = orientdb.open(dbname, login, password);) {
            // create databases
            OClass hospital = db.getClass("Hospital");
            OClass zipDetails = db.getClass("ZipDetails");
            OClass zipDistance = db.getClass("ZipDistance");
            OClass patient = db.getClass("Patient");
            if(hospital == null){
                hospital = createHospitalClass(db, hospital);
            }
            if(zipDetails == null){
                zipDetails = createKYZipDetailsClass(db, zipDetails);
            }
            if(zipDistance == null){
                zipDistance = createKYZipDistanceClass(db, zipDistance);
            }
            if(patient == null){
                patient = createPatientClass(db, patient);
            }
        } catch (Exception e){
            System.out.println(e);
        }
        
        orientdb.close();
    }
}
