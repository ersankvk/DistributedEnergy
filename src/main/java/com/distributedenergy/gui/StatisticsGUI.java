package com.distributedenergy.gui;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.json.JSONObject;

public class StatisticsGUI extends Application {

    private Label producedLabel = new Label("Community Produced: ");
    private Label usedLabel = new Label("Community Used: ");
    private Label gridLabel = new Label("Grid Used: ");
    private Label avgProducedLabel = new Label("Avg Produced: ");
    private Label avgUsedLabel = new Label("Avg Used: ");
    private Label communityDepletedLabel = new Label("Community Depleted: ");
    private Label gridPortionLabel = new Label("Grid Portion: ");
    private Label hourLabel = new Label("Hour: ");

    @Override
    public void start(Stage stage) {
        VBox vbox = new VBox(10, hourLabel, producedLabel, usedLabel, gridLabel, avgProducedLabel, avgUsedLabel, communityDepletedLabel, gridPortionLabel);
        vbox.setStyle("-fx-padding: 20px;");

        Scene scene = new Scene(vbox, 400, 300);
        stage.setScene(scene);
        stage.setTitle("Energie-Statistiken");
        stage.show();

        fetchStatistics();
    }

    private void fetchStatistics() {
        try {
            URL url = new URL("http://localhost:8080/api/statistics");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            reader.close();
            JSONObject json = new JSONObject(response.toString());

            // Werte setzen
            hourLabel.setText("Hour: " + json.optString("hour"));
            producedLabel.setText("Community Produced: " + json.optDouble("produced"));
            usedLabel.setText("Community Used: " + json.optDouble("used"));
            gridLabel.setText("Grid Used: " + json.optDouble("grid"));
            avgProducedLabel.setText("Avg Produced: " + json.optDouble("avg_produced"));
            avgUsedLabel.setText("Avg Used: " + json.optDouble("avg_used"));
            communityDepletedLabel.setText("Community Depleted: " + json.optDouble("community_depleted"));
            gridPortionLabel.setText("Grid Portion: " + json.optDouble("grid_portion"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        launch();
    }
}