package com.distributedenergy.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class ProducerApp {

    private final static String QUEUE_NAME = "energy-queue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        // Verbindung zu RabbitMQ aufbauen (lokal, Standard-Port)
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            // Queue deklarieren (wird nur einmal erstellt)
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            ObjectMapper objectMapper = new ObjectMapper(); // für JSON
            Random random = new Random();

            while (true) {
                // Nachricht aufbauen
                Map<String, Object> message = new HashMap<>();
                message.put("type", "PRODUCER");
                message.put("association", "COMMUNITY");
                message.put("kwh", 0.001 + (0.004 - 0.001) * random.nextDouble()); // realistischer Wert
                message.put("datetime", LocalDateTime.now().toString());

                // Als JSON-String konvertieren
                String jsonMessage = objectMapper.writeValueAsString(message);

                // Debugging-Ausgabe hinzufügen
                System.out.println("Gesendete Nachricht: " + jsonMessage); // Debugging-Ausgabe

                // Nachricht senden
                channel.basicPublish("", QUEUE_NAME, null, jsonMessage.getBytes(StandardCharsets.UTF_8));
                System.out.println("Gesendet: " + jsonMessage);

                // 3 Sekunden warten
                Thread.sleep(3000);
            }
        }
    }
}
