
//import java.io.Console;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
// import java.util.*;
import java.util.concurrent.TimeoutException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

public class BranchOffice2Sync {

    // MySQL database connection parameters
    private static final String DB_URL = "jdbc:mysql://localhost:3306/salesbo2";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

   
  
    private static final String exchangeName = "sales_exchange";
    //private static final String BO1_QUEUE_NAME = "BO1_queue";

    // Gson object for serializing messages to JSON
    //private static final Gson gson = new Gson();


    public static void main(String[] args) {

            // time sync 
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String lastSyncTime = now.format(formatter);
        System.out.println(" Formatted timestamp: "+ lastSyncTime );
            // Gson object for serializing messages to JSON
        Gson gson = new Gson();
        try {
            // Connect to the MySQL database
            Connection conn = DriverManager.getConnection(DB_URL,DB_USER,DB_PASSWORD );

            // Connect to the RabbitMQ server
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            com.rabbitmq.client.Connection rabbitmqConn = factory.newConnection();
            Channel channel = rabbitmqConn.createChannel();

            // Declare the exchange and queue on the RabbitMQ server
           
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
            //channel.queueDeclare(BO1_QUEUE_NAME, false, false, false, null);
            //channel.queueBind(BO1_QUEUE_NAME, exchangeName, "BO1");

            // Create a scheduled task to synchronize data every hour
            /*Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {*/
                    // Query the local sales database for new and modified records
            String query = "SELECT * FROM productsales WHERE  last_sync IS NULL";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();

                // Send each new or modified record to the head office
            while (rs.next()) {
                ProductSale sale = new ProductSale();
                sale.setDate(rs.getString("date"));
                sale.setRegion(rs.getString("region"));
                sale.setProduct(rs.getString("product"));
                sale.setQty(rs.getInt("qty"));
                sale.setCost(rs.getFloat("cost"));
                sale.setAmt(rs.getFloat("amt"));
                sale.setTax(rs.getFloat("tax"));
                sale.setTotal(rs.getFloat("total"));
                System.out.println("dans boucle rs  "+ sale.getDate());
                String json = gson.toJson(sale);
                System.out.println("chaine a envoyer  "+json);
                channel.basicPublish(exchangeName, "BO2" , null, json.getBytes("UTF-8"));
                System.out.println(" [xj sent ' " );
                           
            }

                // Update the last sync time for the branch office
            lastSyncTime = now.format(formatter) ;
                        String query2 = "UPDATE `productsales` SET `last_sync`= ? WHERE last_sync < ? OR last_sync IS NULL";
                        PreparedStatement stmt2 = conn.prepareStatement(query2);
                        stmt2.setString(1,lastSyncTime );
                        stmt2.setString(2,lastSyncTime );
                        stmt2.executeUpdate();
                        System.out.println("query2 done ");

        } catch (SQLException | IOException | TimeoutException ex ) 
        {
            ex.printStackTrace();
        }
    }
} 