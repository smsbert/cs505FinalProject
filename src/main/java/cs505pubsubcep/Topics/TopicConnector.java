package cs505pubsubcep.Topics;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505pubsubcep.Launcher;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import java.io.File;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

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

	    CreateFile();
	    FileWriter myWriter = new FileWriter("inputs.txt", true);
	    myWriter.write("{'patientInfo' : [");
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received Batch'" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                List<Map<String,String>> incomingList = gson.fromJson(message, typeOf);
                //WriteToFile(message);
		
		for(Map<String,String> map : incomingList) {
                    System.out.println("INPUT CEP EVENT: " +  map);
		    
		    myWriter.write(gson.toJson(map) + ", ");
                    
		    Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
                }
                System.out.println("");
                System.out.println("");

            };
	    
	    System.out.println("done?");
	    myWriter.write("]}");
	    myWriter.close();
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
}

  public void CreateFile() {
    try {
      File myObj = new File("inputs.txt");
      if (myObj.createNewFile()) {
        System.out.println("File created: " + myObj.getName());

      } else {
        System.out.println("File already exists.");
      }
    } catch (IOException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
    }
  }

}
