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

import ch.unibas.dmi.dbis.streamteam.evaluation.propertiesHelper.PropertyReadHelper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Component which extracts some selected events from a MongoDB instance.
 */
public class MongoDBEventExtractor {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(MongoDBEventExtractor.class);

    /**
     * Main method of the MongoDB Event Extractor.
     *
     * @param args Parameters
     */
    public static void main(String[] args) {
        String propertiesFilePath = "/evaluationConsumer.properties";

        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = MongoDBEventExtractor.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            logger.error("Unable to load {}", propertiesFilePath, e);
            System.exit(1);
        }

        logger.info("Read properties");
        String connectionString = PropertyReadHelper.readStringOrDie(properties, "mongodb.connectionString");
        String databaseName = PropertyReadHelper.readStringOrDie(properties, "mongodb.database");

        logger.info("Initialize MongoDB");
        MongoClientURI connectionURI = new MongoClientURI(connectionString);
        MongoClient mongoClient = new MongoClient(connectionURI);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> eventsCollection = database.getCollection("events");

        List<String> eventTypes = new ArrayList<>();
        eventTypes.add("interceptionEvent");
        eventTypes.add("successfulPassEvent");
        eventTypes.add("freekickEvent");
        eventTypes.add("cornerkickEvent");
        eventTypes.add("throwinEvent");
        eventTypes.add("goalkickEvent");

        for (String eventType : eventTypes) {
            try {
                FileWriter fileWriter = createFileWriter("streamteamEvents/" + eventType + "s.csv");

                // Header
                if (eventType.equals("interceptionEvent")) {
                    fileWriter.append("ts,endX,endY\n");
                } else if (eventType.equals("successfulPassEvent")) {
                    fileWriter.append("ts,startX,startY,endX,endY\n");
                } else {
                    fileWriter.append("ts,startX,startY\n");
                }

                // https://mongodb.github.io/mongo-java-driver/3.6/driver/tutorials/perform-read-operations/ & https://stackoverflow.com/questions/30424894/java-syntax-with-mongodb
                MongoCursor<Document> cursor = eventsCollection.find(Filters.eq("type", eventType))
                        .sort(Sorts.ascending("ts")).iterator();

                while (cursor.hasNext()) {
                    Document eventDocument = cursor.next();
                    Integer ts = eventDocument.getInteger("ts");
                    List<List<Double>> xyCoordsDocumentList = (List<List<Double>>) eventDocument.get("xyCoords");

                    String line;
                    if (eventType.equals("interceptionEvent")) {
                        line = ts + "," + xyCoordsDocumentList.get(1).get(0) + "," + xyCoordsDocumentList.get(1).get(1);
                    } else if (eventType.equals("successfulPassEvent")) {
                        line = ts + "," + xyCoordsDocumentList.get(0).get(0) + "," + xyCoordsDocumentList.get(0).get(1) + "," + xyCoordsDocumentList.get(1).get(0) + "," + xyCoordsDocumentList.get(1).get(1);
                    } else {
                        // Position of the ball (alternative would be position of the player)
                        line = ts + "," + xyCoordsDocumentList.get(1).get(0) + "," + xyCoordsDocumentList.get(1).get(1);
                    }
                    logger.info("{}: {}", eventType, line);
                    fileWriter.append(line + "\n");
                }

                cursor.close();
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
}
