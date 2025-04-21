package com.distributedenergy.percentage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class PercentageServiceApp {

    private static final String UPDATE_QUEUE = "update-queue";

    public static void main(String[] args) throws Exception {
        // RabbitMQ-Verbindung aufbauen
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (com.rabbitmq.client.Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Queue deklarieren
            channel.queueDeclare(UPDATE_QUEUE, false, false, false, null);
            ObjectMapper mapper = new ObjectMapper();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                Map<String, String> data = mapper.readValue(message, HashMap.class);

                if ("UPDATE".equals(data.get("type"))) {
                    LocalDateTime hour = LocalDateTime.parse(data.get("hour"));

                    // JDBC-Verbindung zur PostgreSQL-Datenbank
                    try (java.sql.Connection db = DriverManager.getConnection(
                            "jdbc:postgresql://localhost:5432/energy", "postgres", "passwort")) {

                        double used = 0;
                        double grid = 0;

                        // SELECT-Abfrage
                        try (PreparedStatement select = db.prepareStatement(
                                "SELECT community_used, grid_used FROM usage WHERE hour = ?")) {

                            select.setTimestamp(1, Timestamp.valueOf(hour));

                            try (ResultSet rs = select.executeQuery()) {
                                if (rs.next()) {
                                    used = rs.getDouble("community_used");
                                    grid = rs.getDouble("grid_used");
                                }
                            }
                        }

                        double total = used + grid;
                        double gridPortion = total > 0 ? (grid / total) * 100 : 0;

                        // INSERT oder UPDATE der Prozentwerte
                        try (PreparedStatement insert = db.prepareStatement(
                                "INSERT INTO percentage (hour, community_depleted, grid_portion) " +
                                        "VALUES (?, ?, ?) " +
                                        "ON CONFLICT (hour) DO UPDATE SET " +
                                        "community_depleted = EXCLUDED.community_depleted, " +
                                        "grid_portion = EXCLUDED.grid_portion")) {

                            insert.setTimestamp(1, Timestamp.valueOf(hour));
                            insert.setDouble(2, used);
                            insert.setDouble(3, gridPortion);
                            insert.executeUpdate();
                        }

                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            };

            // Nachrichten konsumieren
            channel.basicConsume(UPDATE_QUEUE, true, deliverCallback, consumerTag -> {});
            channel.basicConsume(UPDATE_QUEUE, true, deliverCallback, consumerTag -> {});

// Offen halten, damit das Programm nicht beendet wird
            new java.util.concurrent.CountDownLatch(1).await();

        }

    }
}