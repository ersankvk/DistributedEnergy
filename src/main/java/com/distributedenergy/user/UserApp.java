package com.distributedenergy.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class UserApp {

    private final static String QUEUE_NAME = "energy-queue";

    public static void main(String[] args) throws Exception {
        // Verbindung zur RabbitMQ herstellen
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Warteschlange deklarieren
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            ObjectMapper mapper = new ObjectMapper();
            Random random = new Random();

            while (true) {
                // Zufällige, aber plausible Verbrauchsdaten
                double kwh = 0.001 + (0.004 * random.nextDouble()); // z.B. 0.001 - 0.005

                Map<String, Object> message = new HashMap<>();
                message.put("type", "USER");
                message.put("association", "COMMUNITY");
                message.put("kwh", kwh);
                message.put("datetime", LocalDateTime.now().toString());

                // Nachricht serialisieren und senden
                String jsonMessage = mapper.writeValueAsString(message);
                channel.basicPublish("", QUEUE_NAME, null, jsonMessage.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent USER: " + jsonMessage);

                // Warte 1–5 Sekunden
                Thread.sleep(1000 + random.nextInt(4000));
            }
        }
    }
}
