package com.distributedenergy.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@RestController
public class EnergyController {

    // Endpoint für die Startseite
    @GetMapping("/")
    public String home() {
        return "Distributed Energy System is running!";
    }

    // Endpoint für eine einfache Hello-World-Nachricht
    @GetMapping("/api/hello")
    public String hello() {
        return "Hello from the Distributed Energy API!";
    }

    // Endpoint für die Abfrage von aktuellen Statistiken
    @GetMapping("/api/statistics")
    public Map<String, Object> getStatistics() {
        Map<String, Object> response = new HashMap<>();

        try (Connection db = DriverManager.getConnection("jdbc:postgresql://localhost:5432/energy", "postgres", "passwort")) {
            // Neueste Daten aus 'usage'
            try (PreparedStatement stmt = db.prepareStatement("SELECT * FROM usage ORDER BY hour DESC LIMIT 1");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    response.put("hour", rs.getTimestamp("hour").toLocalDateTime().toString());
                    response.put("produced", rs.getDouble("community_produced"));
                    response.put("used", rs.getDouble("community_used"));
                    response.put("grid", rs.getDouble("grid_used"));
                }
            }

            // Prozentsatz aus 'percentage'
            try (PreparedStatement stmt = db.prepareStatement("SELECT * FROM percentage ORDER BY hour DESC LIMIT 1");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    response.put("community_depleted", rs.getDouble("community_depleted"));
                    response.put("grid_portion", rs.getDouble("grid_portion"));
                }
            }

            // Durchschnittswerte aus 'statistics'
            try (PreparedStatement stmt = db.prepareStatement("SELECT * FROM statistics ORDER BY hour DESC LIMIT 1");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    response.put("avg_produced", rs.getDouble("avg_produced"));
                    response.put("avg_used", rs.getDouble("avg_used"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            response.put("error", "Fehler beim Laden der Daten");
        }

        return response;
    }

    // Neu: Endpoint für spezifische Daten aus der 'usage'-Tabelle
    @GetMapping("/api/usage")
    public Map<String, Object> getUsageData() {
        Map<String, Object> response = new HashMap<>();

        try (Connection db = DriverManager.getConnection("jdbc:postgresql://localhost:5432/energy", "postgres", "passwort")) {
            // Neueste Daten aus 'usage'
            try (PreparedStatement stmt = db.prepareStatement("SELECT * FROM usage ORDER BY hour DESC LIMIT 1");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    response.put("hour", rs.getTimestamp("hour").toLocalDateTime().toString());
                    response.put("community_produced", rs.getDouble("community_produced"));
                    response.put("community_used", rs.getDouble("community_used"));
                    response.put("grid_used", rs.getDouble("grid_used"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            response.put("error", "Fehler beim Laden der Daten");
        }

        return response;
    }
}