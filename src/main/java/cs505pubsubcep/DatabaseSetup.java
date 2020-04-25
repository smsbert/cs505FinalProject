package cs505pubsubcep;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;


public class DatabaseSetup {

    private OrientDB orientdb;
    private ODatabaseSession dbSession;

    public boolean reset_db(String name) {
        // Remove Old Database
        if (orientdb.exists(name)) {
            orientdb.drop(name);
        }
        
        // Create New Database
        orientdb.create(name, ODatabaseType.PLOCAL);

        // if successful
        return true;
    }

    // public void printJSONDB(String filepath){
    
    //     BufferedReader reader = new BufferedReader(new FileReader (filepath));
    //     JSONArray data = json.load(reader);
    //     with open(filepath) as f {
    //         data = json.load(f);
    //     }
    
    //     for (Object key : data) {
            
    //         String name = data.get(key).get("name");
    //         String wikiUrl = data.get(key).get("wikiUrl");
    //         String wikiImage = data.get(key).get("wikiImage");
    //         String degreeLists = data.get(key).get("degreeLists");
    
    //         System.out.print("json key:\t\t" + key);
    //         System.out.print("name:\t\t\t" + name);
    
    //         // Most people are on Wikipedia, but not everyone
    //         if (wikiUrl == null) {
    //             System.out.print("wikiUrl:\t\t" + wikiUrl);
    //         }
    //         if (wikiImage == null) {
    //             System.out.print("wikiImage:\t\t" + wikiImage);
    //         }
    
    //         System.out.print(degreeLists);
    //         System.out.print("");
    //     }
    // }

    public void createHospitalClass(OrientGraph graphDB){
        graphDB.command(new OCommandSQL("CREATE VERTEX Hospital EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.id STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.name STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.address STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.city STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.type STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.beds INTEGER;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.county STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.country STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.lattitude FLOAT;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.longitude FLOAT;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.naics STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.website STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.owner STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.trauma STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.helipad BOOLEAN;")).execute();
    }

    public void createKYZipDetailsClass(OrientGraph graphDB){
        graphDB.command(new OCommandSQL("CREATE VERTEX ZipDetails EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.zip STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.zipcodeName STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.city STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.state STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.countyName STRING;")).execute();

    }

    public void createKYZipDistanceClass(OrientGraph graphDB){
        graphDB.command(new OCommandSQL("CREATE VERTEX ZipDistamce EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDistamce.zipFrom STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDistamce.zipTo STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDistamce.distance FLOAT;")).execute();
    }

    public void createPatientClass(OrientGraph graphDB){
        graphDB.command(new OCommandSQL("CREATE VERTEX Patient EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.firstName STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.lastName STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.mrn STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.zipcode STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.statusCode STRING;")).execute();
    }

    
    public void loadDB(String filepath){
    
        // database name
        String dbname = "patient";
        // database login is root by default
        String login = "root";
        // database password, set by docker param
        String password = "rootpwd";

        OrientGraph graphDB = new OrientGraph("localhost:patient", login, password);
        // graphDB.createVertexType("Patient");

        // create databases
        // createHospitalClass(graphDB);
        // createKYZipDetailsClass(graphDB);
        // createKYZipDistanceClass(graphDB);
        // createPatientClass(graphDB);

        graphDB.command(new OCommandSQL("CREATE VERTEX Hospital EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.id STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.name STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.address STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.city STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.state STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.zip STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.type STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.beds INTEGER;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.county STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.country STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.lattitude FLOAT;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.longitude FLOAT;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.naics STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.website STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.owner STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.trauma STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Hospital.helipad BOOLEAN;")).execute();

        graphDB.command(new OCommandSQL("CREATE VERTEX ZipDetails EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.zip STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.zipcodeName STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.city STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.state STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDetails.countyName STRING;")).execute();

        graphDB.command(new OCommandSQL("CREATE VERTEX ZipDistamce EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDistamce.zipFrom STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDistamce.zipTo STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY ZipDistamce.distance FLOAT;")).execute();

        graphDB.command(new OCommandSQL("CREATE VERTEX Patient EXTENDS V;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.firstName STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.lastName STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.mrn STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.zipcode STRING;")).execute();
        graphDB.command(new OCommandSQL("CREATE PROPERTY Patient.statusCode STRING;")).execute();

        // create client to connect to local orientdb docker container
        // orientdb = new OrientDB("localhost:2424", login, password, OrientDBConfig.defaultConfig());
        // // session_id = client.connect(login, password);
        
        // // remove old database and create new one
        // reset_db(dbname);
        
        // // open the database we are interested in
        // orientdb.open(dbname, login, password);
        // // client.db_open(dbname, login, password);
    
        // // open and parse local json file
        // with open(filepath) as f {
        //     data = json.load(f)''
        // }
    
    
        // // loop through each key in the json database and create a new vertex, V with the id in the database
        // for (Object key : Data) {
        //     firstname = data.get(key).get("first_name");
        //     lastName = data.get(key).get("last_name");
        //     mrn = data.get(key).get("mrn");
        //     zipcode = str(data.get(key).get("zip_code"));
        //     statusCode = str(data.get(key).get("patient_status_code"));
    
        //     client.command("CREATE VERTEX Person SET firstName = '" + firstName + "', lastName = '" + lastName + "', mrn = '" + mrn + "', zipcode = '" + zipcode + "', statusCode = '" + statusCode + "'");
        // }
    
    
        // loop through each key creating edges from advisor to advise
        // for (Object key : Data) {
        //     advisorNodeId = str(getrid(client,key));
        //     for student in data.get(key)["students"] {
        //         studentNodeId = str(getrid(client,student));
        //         client.command("CREATE EDGE FROM " + advisorNodeId + " TO " + studentNodeId);
        //     }
        // }
    
        // graphDB.close();
    }

}

