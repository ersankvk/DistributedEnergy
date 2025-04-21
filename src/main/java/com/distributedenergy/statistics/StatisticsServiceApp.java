package com.distributedenergy.statistics;

import java.sql.*;

public class StatisticsServiceApp {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/energy";
        String user = "postgres";
        String password = "passwort";

        try (Connection db = DriverManager.getConnection(url, user, password)) {

            System.out.println("Aktuelle Energie-Statistiken:");

            // Letzter Eintrag aus der usage-Tabelle holen
            String query = "SELECT * FROM usage ORDER BY hour DESC LIMIT 1";

            try (PreparedStatement stmt = db.prepareStatement(query);
                 ResultSet rs = stmt.executeQuery()) {

                if (rs.next()) {
                    Timestamp timestamp = rs.getTimestamp("hour");
                    double produced = rs.getDouble("community_produced");
                    double used = rs.getDouble("community_used");
                    double grid = rs.getDouble("grid_used");

                    double totalConsumption = used + grid;
                    double selfSupply = totalConsumption > 0 ? (used / totalConsumption) * 100 : 0;
                    double gridSupply = totalConsumption > 0 ? (grid / totalConsumption) * 100 : 0;

                    System.out.println("Stunde: " + timestamp.toLocalDateTime());
                    System.out.printf("Erzeugt: %.3f kWh\n", produced);
                    System.out.printf("Genutzt (Community): %.3f kWh\n", used);
                    System.out.printf("Aus Netz: %.3f kWh\n", grid);
                    System.out.printf("Eigenversorgung: %.2f %%\n", selfSupply);
                    System.out.printf("Netzanteil: %.2f %%\n", gridSupply);
                } else {
                    System.out.println("Keine Einträge in der Tabelle 'usage' gefunden.");
                }

            }

        } catch (SQLException e) {
            System.err.println("Fehler bei der Datenbankverbindung:");
            e.printStackTrace();
        }
    }
}