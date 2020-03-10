/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.streamteam.evaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Component which consumes calculates the latencies for some selected data streams using the send system time and receive system time CSV files
 */
public class LatencyCalculator {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(LatencyCalculator.class);

    /**
     * Main method of the Latency Calculator.
     *
     * @param args Parameters
     */
    public static void main(String[] args) {
        String propertiesFilePath = "/evaluationConsumer.properties";

        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = LatencyCalculator.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            logger.error("Unable to load {}.", propertiesFilePath, e);
            System.exit(1);
        }

        Map<String, Long> minimumSendSystemTimeMap = null;
        try {
            minimumSendSystemTimeMap = constructMinimumSendSystemTimeMap("../streamteam-sensor-simulator/log/rawPositionSensorDataSendSystemTimes.csv");

            // https://stackoverflow.com/questions/3634853/how-to-create-a-directory-in-java
            new File("./latencies").mkdirs();

            constructAndWriteLatencyFile(minimumSendSystemTimeMap, "./log/ballObjectStateReceiveSystemTimes.csv", "./latencies/ballObjectStateLatencies.csv");
            constructAndWriteLatencyFile(minimumSendSystemTimeMap, "./log/A1FullGameHeatmapStatisticsReceiveSystemTimes.csv", "./latencies/A1FullGameHeatmapStatisticsLatencies.csv");
            constructAndWriteLatencyFile(minimumSendSystemTimeMap, "./log/kickEventReceiveSystemTimes.csv", "./latencies/kickEventLatencies.csv");
            constructAndWriteLatencyFile(minimumSendSystemTimeMap, "./log/BPassStatisticsReceiveSystemTimes.csv", "./latencies/BPassStatisticsLatencies.csv");
            constructAndWriteLatencyFile(minimumSendSystemTimeMap, "./log/passSequenceEventReceiveSystemTimes.csv", "./latencies/passSequenceEventLatencies.csv");
        } catch (IOException e) {
            logger.error("Unable to calculate latencies.", e);
            System.exit(1);
        }
    }

    /**
     * Constructs a map containing the minimum system time at which a raw position sensor data stream element was sent for each matchId-generationTs-combination.
     *
     * @param sendSystemTimeFilePath Path to send system time file
     * @return Map containing the minimum system time at which a raw position sensor data stream element was sent for each matchId-generationTs-combination
     * @throws IOException Thrown if the send system time file could not be read properly
     */
    public static Map<String, Long> constructMinimumSendSystemTimeMap(String sendSystemTimeFilePath) throws IOException {
        Map<String, Long> minimumSendSystemTimeMap = new HashMap<>();

        FileReader fileReader = new FileReader(sendSystemTimeFilePath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        bufferedReader.readLine(); // skip header

        String line = bufferedReader.readLine();
        while (line != null) {
            String[] lineParts = line.split(",");
            String key = lineParts[0] + "-" + lineParts[1];
            Long value = new Long(lineParts[2]);

            if (minimumSendSystemTimeMap.containsKey(key)) {
                value = Math.min(value, minimumSendSystemTimeMap.get(key));
            }
            minimumSendSystemTimeMap.put(key, value);

            line = bufferedReader.readLine();
        }

        bufferedReader.close();
        fileReader.close();

        return minimumSendSystemTimeMap;
    }

    /**
     * Constructs and writes a file containing all latencies for a certain data stream.
     *
     * @param minimumSendSystemTimeMap  Map containing the minimum system time at which a raw position sensor data stream element was sent for each matchId-generationTs-combination
     * @param receiveSystemTimeFilePath Path to receive system time file
     * @param latencyFilePath           Path to latency file
     * @throws IOException Thrown if the receive system time file could not be read properly or the latency file could not be written properly
     */
    public static void constructAndWriteLatencyFile(Map<String, Long> minimumSendSystemTimeMap, String receiveSystemTimeFilePath, String latencyFilePath) throws IOException {
        FileWriter fileWriter = new FileWriter(latencyFilePath, false);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.append("matchId,generationTimestamp,latencyInMs\n"); // Header

        FileReader fileReader = new FileReader(receiveSystemTimeFilePath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        bufferedReader.readLine(); // skip header

        String line = bufferedReader.readLine();
        while (line != null) {
            String[] lineParts = line.split(",");
            String key = lineParts[0] + "-" + lineParts[1];
            Long receiveSystemTime = new Long(lineParts[2]);

            if (minimumSendSystemTimeMap.containsKey(key)) {
                Long minimumSendSystemTime = minimumSendSystemTimeMap.get(key);
                Long latency = receiveSystemTime - minimumSendSystemTime;
                bufferedWriter.append(lineParts[0] + "," + lineParts[1] + "," + latency + "\n");
            } else {
                logger.error("Skipped key {} since there was no value in the minimumSendSystemTimeMap.", key);
            }

            line = bufferedReader.readLine();
        }

        bufferedReader.close();
        fileReader.close();

        bufferedWriter.close();
        fileWriter.close();
    }
}
