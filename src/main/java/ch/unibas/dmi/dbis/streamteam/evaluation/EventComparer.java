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
import java.util.*;

/**
 * Component which compares the events detected by StreamTeam-Football (extracted from a MongoDB instance with the MongoDBEventExtractor) with the events extracted from an OPTA dataset (with the OptaEventExtractor).
 */
public class EventComparer {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(EventComparer.class);

    /**
     * Main method of the Event Comparer.
     *
     * @param args Parameters
     */
    public static void main(String[] args) {
        String propertiesFilePath = "/evaluationConsumer.properties";

        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = EventComparer.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            logger.error("Unable to load {}.", propertiesFilePath, e);
            System.exit(1);
        }

        List<String> eventTypes = new ArrayList<>();
        eventTypes.add("interceptionEvent");
        eventTypes.add("successfulPassEvent");
        eventTypes.add("freekickEvent");
        eventTypes.add("cornerkickEvent");
        eventTypes.add("throwinEvent");
        eventTypes.add("goalkickEvent");

        List<Integer> timeThresholds = new ArrayList<>();
        timeThresholds.add(1000);
        timeThresholds.add(2000);
        timeThresholds.add(3000);
        timeThresholds.add(4000);
        timeThresholds.add(5000);

        List<Double> distThresholds = new ArrayList<>();
        distThresholds.add(1.0);
        distThresholds.add(3.0);
        distThresholds.add(5.0);
        distThresholds.add(7.0);
        distThresholds.add(9.0);
        distThresholds.add(Double.MAX_VALUE);

        for (String eventType : eventTypes) {
            try {
                List<String> streamTeamEventLines = readFileToList("streamteamEvents/" + eventType + "s.csv");
                List<String> optaEventLines = readFileToList("optaEvents/" + eventType + "s.csv");

                FileWriter fileWriter = createFileWriter("qualitativeEvalStats/" + eventType + "Stats.csv");
                // Header
                fileWriter.append("timeThreshold,distanceThreshold,correctDetections,wrongDetections,missedDetections,correctDetectionsPercentage,wrongDetectionsPercentage,missedDetectionsPercentage\n");

                for (Integer timeThreshold : timeThresholds) {
                    for (Double distThreshold : distThresholds) {
                        int correctDetections = 0;

                        Set<String> streamTeamEventLinesSet = new HashSet<>();
                        streamTeamEventLinesSet.addAll(streamTeamEventLines);
                        Set<String> optaEventLinesSet = new HashSet<>();
                        optaEventLinesSet.addAll(optaEventLines);

                        for (String streamTeamEventLine : streamTeamEventLines) {
                            optaEventLoop:
                            for (String optaEventLine : optaEventLines) {
                                if (optaEventLinesSet.contains(optaEventLine)) { // prevent that the same OPTA event is the matching event for multiple StreamTeam events
                                    if (compareEventLines(streamTeamEventLine, optaEventLine, eventType, timeThreshold, distThreshold)) {
                                        correctDetections++;
                                        streamTeamEventLinesSet.remove(streamTeamEventLine);
                                        optaEventLinesSet.remove(optaEventLine);
                                        break optaEventLoop;
                                    }
                                }
                            }
                        }

                        int wrongDetections = streamTeamEventLinesSet.size();
                        int missedDetections = optaEventLinesSet.size();

                        double correctDetectionsPercentage = ((double) correctDetections) / streamTeamEventLines.size();
                        double wrongDetectionsPercentage = ((double) wrongDetections) / streamTeamEventLines.size();
                        double missedDetectionsPercentage = ((double) missedDetections) / optaEventLines.size();

                        fileWriter.append(timeThreshold + "," + distThreshold + "," + correctDetections + "," + wrongDetections + "," + missedDetections + "," + correctDetectionsPercentage + "," + wrongDetectionsPercentage + "," + missedDetectionsPercentage + "\n");
                    }
                }

                fileWriter.close();
            } catch (IOException e) {
                logger.error("Caught exception.", e);
            }
        }
    }

    /**
     * Creates a non-appending file writer and the directory.
     *
     * @param path File path
     * @return Non-appending file writer
     * @throws IOException Thrown if an IOException is thrown while creating the file write or the directory.
     */
    private static FileWriter createFileWriter(String path) throws IOException {
        File successfulPassEventsFile = new File(path);
        successfulPassEventsFile.getParentFile().mkdirs();
        return new FileWriter(successfulPassEventsFile, false);
    }

    /**
     * Reads a file containing to a list containing all lines of the file except for the header.
     *
     * @param path Path to  file
     * @return List containing all the lines of the file except for the header
     * @throws IOException Thrown if the  file could not be read properly
     */
    public static List<String> readFileToList(String path) throws IOException {
        List<String> list = new LinkedList<>();

        FileReader fileReader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        bufferedReader.readLine(); // skip header

        String line = bufferedReader.readLine();
        while (line != null) {
            list.add(line);

            line = bufferedReader.readLine();
        }

        bufferedReader.close();
        fileReader.close();

        return list;
    }

    /**
     * Compare a StreamTeam event with an OPTA event.
     *
     * @param streamTeamEventLine Line describing the StreamTeam event
     * @param optaEventLine       Line describing the OPTA event
     * @param eventType           Event type
     * @param timeThreshold       Time threshold (in ms)
     * @param distThreshold       Distance threshold (in m)
     * @return True if the StreamTeam event matches the OPTA event, otherwise false
     */
    private static boolean compareEventLines(String streamTeamEventLine, String optaEventLine, String eventType, int timeThreshold, double distThreshold) {
        String[] splittedStreamTeamEventLine = streamTeamEventLine.split(",");
        String[] splittedOptaEventLine = optaEventLine.split(",");

        int streamTeamTs = Integer.parseInt(splittedStreamTeamEventLine[0]);
        int optaTs = Integer.parseInt(splittedOptaEventLine[0]);
        if (Math.abs(streamTeamTs - optaTs) < timeThreshold) {
            double streamTeamX = Double.parseDouble(splittedStreamTeamEventLine[1]);
            double streamTeamY = Double.parseDouble(splittedStreamTeamEventLine[2]);
            double optaX = Double.parseDouble(splittedOptaEventLine[1]);
            double optaY = Double.parseDouble(splittedOptaEventLine[2]);
            if (euclideanDist(streamTeamX, streamTeamY, optaX, optaY) < distThreshold) {
                if (eventType.equals("successfulPassEvent")) {
                    double streamTeamX2 = Double.parseDouble(splittedStreamTeamEventLine[3]);
                    double streamTeamY2 = Double.parseDouble(splittedStreamTeamEventLine[4]);
                    double optaX2 = Double.parseDouble(splittedOptaEventLine[3]);
                    double optaY2 = Double.parseDouble(splittedOptaEventLine[4]);
                    if (euclideanDist(streamTeamX2, streamTeamY2, optaX2, optaY2) < distThreshold) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Calculates the eucildean distance between two positions.
     *
     * @param x1 X-coordinate of the first position
     * @param y1 Y-Coordinate of the first position
     * @param x2 X-coordinate of the second position
     * @param y2 Y-coordinate of the second position
     * @return Euclidean distance
     */
    private static double euclideanDist(double x1, double y1, double x2, double y2) {
        return Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
    }
}
