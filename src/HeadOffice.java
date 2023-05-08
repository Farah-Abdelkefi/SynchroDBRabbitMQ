
import com.rabbitmq.client.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

public class HeadOffice {

    private static final String BO1_QUEUE_NAME = "BO1_queue";
    private static final String BO2_QUEUE_NAME = "BO2_queue";
    static String exchangeName = "sales_exchange";

    private static final String DB_URL = "jdbc:mysql://localhost:3306/sales";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) throws IOException, TimeoutException {
        try {
                // Set up RabbitMQ connection and channel
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            com.rabbitmq.client.Connection connection =  factory.newConnection();
            Channel channel = connection.createChannel();
        
            System.out.println("BO1_QUEUE_NAME");
                // Declare the queues for the two branch offices
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
            channel.queueDeclare(BO1_QUEUE_NAME, false, false, false, null);
            channel.queueDeclare(BO2_QUEUE_NAME, false, false, false, null);

            
                // Bind the queues to the exchange
            channel.queueBind(BO1_QUEUE_NAME, exchangeName, "BO1");
            channel.queueBind(BO2_QUEUE_NAME, exchangeName, "BO2");
            System.out.println("Listening for messages on queues");
            
                // Create a message consumer for  queue
            DeliverCallback deliverCallback = (consumerTag ,delivery) -> 
            {
                System.out.println("dans call back to consume queue");
                String message = new String(delivery.getBody(), "UTF-8");
                    
                    // Extract the data from the message
                System.out.println("received message " + message);
                String[] messageParts = message.split(",");
            
                String date = messageParts[0].split(":")[1].trim();
                String region = messageParts[1].split(":")[1].trim();
                String product = messageParts[2].split(":")[1].trim();
                int qty = Integer.parseInt(messageParts[3].split(":")[1].trim());
                float cost = Float.parseFloat(messageParts[4].split(":")[1].trim());
                float amt = Float.parseFloat(messageParts[5].split(":")[1].trim());
                float tax = Float.parseFloat(messageParts[6].split(":")[1].trim());
                float total = Float.parseFloat(messageParts[7].split(":")[1].split("}")[0].trim());
        
                    // Update the sales database with the new data from BO1
                try (Connection dbConnection = DriverManager.getConnection(DB_URL,DB_USER,DB_PASSWORD)) {
                    String query = "INSERT INTO productsales (Date, Region, Product, Qty, Cost, Amt, Tax, Total) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                    PreparedStatement preparedStatement = dbConnection.prepareStatement(query);
                    preparedStatement.setString(1, date);
                    preparedStatement.setString(2, region);
                    preparedStatement.setString(3, product);
                    preparedStatement.setInt(4, qty);
                    preparedStatement.setFloat(5, cost);
                    preparedStatement.setFloat(6, amt);
                    preparedStatement.setFloat(7, tax);
                    preparedStatement.setFloat(8, total);
                    preparedStatement.executeUpdate();
                    System.out.println("data added to database");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };

                // consume the queues
            channel.basicConsume(BO1_QUEUE_NAME, true, deliverCallback, consumerTag -> { } );
            channel.basicConsume(BO2_QUEUE_NAME , true, deliverCallback, consumerTag -> { } );
      
        } catch (Exception e) 
        {
        System.err.println("RabbitMQ listener setup failed: " + e.getMessage());
        }

    }
}