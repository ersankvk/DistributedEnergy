package com.distributedenergy.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class UsageServiceApp {

    private static final String QUEUE_NAME = "energy-queue";
    private static final String UPDATE_QUEUE = "update-queue";

    public static void main(String[] args) throws Exception {
        // RabbitMQ-Verbindung aufbauen
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Verbindung zu RabbitMQ aufbauen
        try (com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
             Channel channel = rabbitConnection.createChannel()) {

            // Queues deklarieren
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueDeclare(UPDATE_QUEUE, false, false, false, null);

            ObjectMapper mapper = new ObjectMapper();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("📩 Nachricht empfangen: " + message); // Debugging-Ausgabe

                Map<String, Object> data = mapper.readValue(message, HashMap.class);

                String type = (String) data.get("type");
                String association = (String) data.get("association");
                double kwh = ((Number) data.get("kwh")).doubleValue();
                String datetimeStr = (String) data.get("datetime");
                LocalDateTime datetime = LocalDateTime.parse(datetimeStr).truncatedTo(ChronoUnit.HOURS);

                try (Connection db = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:5432/energy", "postgres", "passwort");
                     PreparedStatement ps = db.prepareStatement(
                             "INSERT INTO usage (hour, community_produced, community_used, grid_used) " +
                                     "VALUES (?, ?, ?, ?) " +
                                     "ON CONFLICT (hour) DO UPDATE SET " +
                                     "community_produced = usage.community_produced + EXCLUDED.community_produced, " +
                                     "community_used = usage.community_used + EXCLUDED.community_used, " +
                                     "grid_used = usage.grid_used + EXCLUDED.grid_used")) {

                    double produced = 0;
                    double used = 0;
                    double gridUsed = 0;

                    if ("PRODUCER".equalsIgnoreCase(type)) {
                        produced = kwh;
                    } else if ("USER".equalsIgnoreCase(type)) {
                        used = kwh;
                        gridUsed = kwh;
                    }

                    // Manuelle Testwerte für `community_produced`
                    if (produced == 0) {
                        produced = 0.005; // Beispielwert für erzeugte Energie
                    }
                    if (used == 0) {
                        used = 0.003; // Beispielwert für verbrauchte Energie
                    }

                    ps.setTimestamp(1, Timestamp.valueOf(datetime));
                    ps.setDouble(2, produced);
                    ps.setDouble(3, used);
                    ps.setDouble(4, gridUsed);
                    ps.executeUpdate();

                    // UPDATE-Nachricht an RabbitMQ senden
                    Map<String, String> updateMessage = new HashMap<>();
                    updateMessage.put("type", "UPDATE");
                    updateMessage.put("hour", datetime.toString());
                    String updateJson = mapper.writeValueAsString(updateMessage);
                    channel.basicPublish("", UPDATE_QUEUE, null, updateJson.getBytes(StandardCharsets.UTF_8));

                    System.out.println("✅ Datensatz verarbeitet & Update gesendet für: " + datetime);

                } catch (SQLException e) {
                    System.err.println("❌ Fehler bei der Datenbankverbindung:");
                    e.printStackTrace();
                }
            };

            System.out.println("🔄 UsageServiceApp lauscht auf '" + QUEUE_NAME + "'...");
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        }
        // Offen halten, damit Programm nicht beendet wird
        new java.util.concurrent.CountDownLatch(1).await();
    }
}
