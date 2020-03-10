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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Component which extracts some selected events from an Opta dataset.
 */
public class OptaEventExtractor {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(OptaEventExtractor.class);

    /**
     * OPTA event timestamp date format
     */
    private static DateFormat df;

    /**
     * Main method of the Opta Event Extractor.
     *
     * @param args f24FilePath, fieldLength, fieldWidth, mirrorX, and mirrorY
     */
    public static void main(String[] args) {
        String f24FilePath = args[0];
        logger.info("F24 file: {}", f24FilePath);
        double fieldLength = Double.parseDouble(args[1]);
        logger.info("Field length: {}", fieldLength);
        double fieldWidth = Double.parseDouble(args[2]);
        logger.info("Field width: {}", fieldWidth);
        boolean mirrorX = Boolean.parseBoolean(args[3]);
        logger.info("Mirror x-coordinates: {}", mirrorX);
        boolean mirrorY = Boolean.parseBoolean(args[4]);
        logger.info("Mirror y-coordinates: {}", mirrorY);

        // https://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format-with-date-hour-and-minute/3914973
        TimeZone tz = TimeZone.getTimeZone("UTC");
        df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        df.setTimeZone(tz);

        try {
            logger.info("Starts reading F24 file.");
            // https://www.tutorialspoint.com/java_xml/java_dom_parse_document.htm
            File f24File = new File(f24FilePath);
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(f24File);
            NodeList gameNodes = document.getElementsByTagName("Game");
            Element gameElement = (Element) gameNodes.item(0);
            NodeList eventNodes = gameElement.getElementsByTagName("Event");

            FileWriter successfulPassEventsFileWriter = createFileWriter("optaEvents/successfulPassEvents.csv");
            successfulPassEventsFileWriter.append("ts,startX,startY,endX,endY\n"); // Header
            FileWriter interceptionEventsFileWriter = createFileWriter("optaEvents/interceptionEvents.csv");
            interceptionEventsFileWriter.append("ts,endX,endY\n"); // Header
            FileWriter freekickEventsFileWriter = createFileWriter("optaEvents/freekickEvents.csv");
            freekickEventsFileWriter.append("ts,startX,startY\n"); // Header
            FileWriter cornerkickEventsFileWriter = createFileWriter("optaEvents/cornerkickEvents.csv");
            cornerkickEventsFileWriter.append("ts,startX,startY\n"); // Header
            FileWriter throwinEventsFileWriter = createFileWriter("optaEvents/throwinEvents.csv");
            throwinEventsFileWriter.append("ts,startX,startY\n"); // Header
            FileWriter goalkickEventsFileWriter = createFileWriter("optaEvents/goalkickEvents.csv");
            goalkickEventsFileWriter.append("ts,startX,startY\n"); // Header

            Long matchStartUnixTsInMs = null;

            for (int i = 0; i < eventNodes.getLength(); ++i) {
                Element eventElement = (Element) eventNodes.item(i);

                if (eventElement.getAttribute("period_id").equals("1")) { // Event was in the first halftime
                    String typeId = eventElement.getAttribute("type_id");

                    if (typeId.equals("32") && matchStartUnixTsInMs == null) { // First halftime start event
                        // https://stackoverflow.com/questions/7784421/getting-unix-timestamp-from-date
                        matchStartUnixTsInMs = df.parse(eventElement.getAttribute("timestamp")).getTime();
                        logger.info("First half start: {} -> {}", eventElement.getAttribute("timestamp"), matchStartUnixTsInMs);
                    } else if (typeId.equals("8")) { // Interception
                        long ts = transformOptaEventTimestampToMsSinceMatchStart(eventElement.getAttribute("timestamp"), matchStartUnixTsInMs);
                        double endX = getX(eventElement.getAttribute("x"), fieldLength, mirrorX);
                        double endY = getY(eventElement.getAttribute("y"), fieldWidth, mirrorY);
                        logger.info("Interception: {},{},{}", new Object[]{ts, endX, endY});
                        interceptionEventsFileWriter.write(ts + "," + endX + "," + endY + "\n");
                    } else if (typeId.equals("1") || typeId.equals("2")) { // Successful pass, blocked pass (->interception), free kick, corner kick, throwin, or goal kick
                        long ts = transformOptaEventTimestampToMsSinceMatchStart(eventElement.getAttribute("timestamp"), matchStartUnixTsInMs);
                        double startX = getX(eventElement.getAttribute("x"), fieldLength, mirrorX);
                        double startY = getY(eventElement.getAttribute("y"), fieldWidth, mirrorY);

                        String outcome = eventElement.getAttribute("outcome");

                        NodeList qNodes = eventElement.getElementsByTagName("Q");
                        boolean hasQualifierId5 = false;
                        boolean hasQualifierId6 = false;
                        boolean hasQualifierId107 = false;
                        boolean hasQualifierId124 = false;
                        boolean hasQualifierId236 = false;
                        Double endX = null;
                        Double endY = null;
                        for (int j = 0; j < qNodes.getLength(); ++j) {
                            Element qElement = (Element) qNodes.item(j);
                            switch (qElement.getAttribute("qualifier_id")) {
                                case "5":
                                    hasQualifierId5 = true;
                                    break;
                                case "6":
                                    hasQualifierId6 = true;
                                    break;
                                case "107":
                                    hasQualifierId107 = true;
                                    break;
                                case "124":
                                    hasQualifierId124 = true;
                                    break;
                                case "140":
                                    endX = getX(qElement.getAttribute("value"), fieldLength, mirrorX);
                                    break;
                                case "141":
                                    endY = getY(qElement.getAttribute("value"), fieldWidth, mirrorY);
                                    break;
                                case "236":
                                    hasQualifierId236 = true;
                                    break;
                            }
                        }

                        if (!hasQualifierId5 && !hasQualifierId6 && !hasQualifierId107 && !hasQualifierId124 && !hasQualifierId236 && endX != null && endY != null && outcome.equals("1")) { // Successful pass
                            logger.info("SuccessfulPass: {},{},{},{},{}", new Object[]{ts, startX, startY, endX, endY});
                            successfulPassEventsFileWriter.write(ts + "," + startX + "," + startY + "," + endX + "," + endY + "\n");
                        } else if (hasQualifierId5 && !hasQualifierId6 && !hasQualifierId107 && !hasQualifierId124 && !hasQualifierId236) { // Freekick
                            logger.info("Freekick: {},{},{}", new Object[]{ts, startX, startY});
                            freekickEventsFileWriter.write(ts + "," + startX + "," + startY + "\n");
                        } else if (!hasQualifierId5 && hasQualifierId6 && !hasQualifierId107 && !hasQualifierId124 && !hasQualifierId236) { // Cornerkick
                            logger.info("Cornerkick: {},{},{}", new Object[]{ts, startX, startY});
                            cornerkickEventsFileWriter.write(ts + "," + startX + "," + startY + "\n");
                        } else if (!hasQualifierId5 && !hasQualifierId6 && hasQualifierId107 && !hasQualifierId124 && !hasQualifierId236) { // Throwin
                            logger.info("Throwin: {},{},{}", new Object[]{ts, startX, startY});
                            throwinEventsFileWriter.write(ts + "," + startX + "," + startY + "\n");
                        } else if (!hasQualifierId5 && !hasQualifierId6 && !hasQualifierId107 && hasQualifierId124 && !hasQualifierId236) { // Goalkick
                            logger.info("Goalkick: {},{},{}", new Object[]{ts, startX, startY});
                            goalkickEventsFileWriter.write(ts + "," + startX + "," + startY + "\n");
                        } else if (!hasQualifierId5 && !hasQualifierId6 && !hasQualifierId107 && !hasQualifierId124 && hasQualifierId236 && endX != null && endY != null) { // Blocked pass -> interception
                            logger.info("Interception: {},{},{}", new Object[]{ts, endX, endY});
                            interceptionEventsFileWriter.write(ts + "," + endX + "," + endY + "\n");
                        } else {
                            logger.info("Ignored event with type_id 1 or 2 but unknown qualifier_id and outcome combination.");
                        }
                    }
                }
            }

            interceptionEventsFileWriter.close();
            successfulPassEventsFileWriter.close();
            freekickEventsFileWriter.close();
            cornerkickEventsFileWriter.close();
            throwinEventsFileWriter.close();
            goalkickEventsFileWriter.close();
        } catch (SAXException | ParserConfigurationException | IOException | ParseException e) {
            logger.error("Caught exception.", e);
        }
    }

    /**
     * Transforms the OPTA event timestamp into a timestamp specifying the time in ms since the start of the match.
     *
     * @param optaTs               OPTA event timestamp String
     * @param matchStartUnixTsInMs Match start UNIX timestamp in ms
     * @return Time in ms since the start of the match
     */
    private static long transformOptaEventTimestampToMsSinceMatchStart(String optaTs, long matchStartUnixTsInMs) throws ParseException {
        Date optaTsDate = df.parse(optaTs);
        long unixTsInMs = optaTsDate.getTime();
        return unixTsInMs - matchStartUnixTsInMs;
    }

    /**
     * Transforms an OPTA x-coordinate String into the StreamTeam-Football coordinate system.
     *
     * @param optaXString OPTA x-coordinate String
     * @param fieldLength Length of the field in m
     * @param mirrorX     Flag which indicates if the x-coordinate has to be mirrored
     * @return X-coordinate according the StreamTeam-Football coordinate system.
     */
    private static double getX(String optaXString, double fieldLength, boolean mirrorX) {
        double x = ((Double.parseDouble(optaXString) - 50) / 100) * fieldLength;
        if (mirrorX) {
            return -x;
        } else {
            return x;
        }
    }

    /**
     * Transforms an OPTA x-coordinate String into the StreamTeam-Football coordinate system.
     *
     * @param optaYString OPTA y-coordinate String
     * @param fieldWidth  Width of the field in m
     * @param mirrorY     Flag which indicates if the y-coordinate has to be mirrored
     * @return Y-coordinate according the StreamTeam-Football coordinate system.
     */
    private static double getY(String optaYString, double fieldWidth, boolean mirrorY) {
        double y = ((Double.parseDouble(optaYString) - 50) / 100) * fieldWidth;
        if (mirrorY) {
            return -y;
        } else {
            return y;
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
}
