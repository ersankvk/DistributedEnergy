package com.distributedenergy.service;

import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@Service
public class EnergyService {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/energy";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "passwort";

    // Methode zur Berechnung der aktuellen Energieverbrauchsdaten
    public Map<String, Object> getEnergyStats() {
        Map<String, Object> response = new HashMap<>();

        try (Connection db = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            // Neueste Daten aus 'usage' Tabelle abfragen
            String query = "SELECT * FROM usage ORDER BY hour DESC LIMIT 1";
            try (PreparedStatement stmt = db.prepareStatement(query);
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    response.put("hour", rs.getTimestamp("hour").toLocalDateTime().toString());
                    response.put("produced", rs.getDouble("community_produced"));
                    response.put("used", rs.getDouble("community_used"));
                    response.put("grid", rs.getDouble("grid_used"));
                }
            }

            // Prozentsatz aus der 'percentage' Tabelle abfragen
            query = "SELECT * FROM percentage ORDER BY hour DESC LIMIT 1";
            try (PreparedStatement stmt = db.prepareStatement(query);
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    response.put("community_depleted", rs.getDouble("community_depleted"));
                    response.put("grid_portion", rs.getDouble("grid_portion"));
                }
            }

            // Durchschnittswerte aus der 'statistics' Tabelle abfragen
            query = "SELECT * FROM statistics ORDER BY hour DESC LIMIT 1";
            try (PreparedStatement stmt = db.prepareStatement(query);
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
}