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

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.*;
import ch.unibas.dmi.dbis.streamteam.evaluation.propertiesHelper.PropertyReadHelper;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Component which consumes selected data stream elements from Kafka and logs the system time when they are received to dedicated CSV files
 */
public class EvaluationConsumer {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(EvaluationConsumer.class);

    /**
     * Parent receive system time marker
     */
    private static final Marker parentReceiveSystemTimeMarker = MarkerFactory.getMarker("PARENT_RECEIVESYSTEMTIME");

    /**
     * Ball object state receive system time marker
     */
    private static final Marker ballObjectStateReceiveSystemTimeMarker = MarkerFactory.getMarker("BALLOBJECTSTATE_RECEIVESYSTEMTIME");

    /**
     * A1 full game heatmap statistics receive system time marker
     */
    private static final Marker A1FullGameHeatmapStatisticsReceiveSystemTimeMarker = MarkerFactory.getMarker("A1FULLGAMEHEATMAPSTATISTICS_RECEIVESYSTEMTIME");

    /**
     * Kick event receive system time marker
     */
    private static final Marker kickEventReceiveSystemTimeMarker = MarkerFactory.getMarker("KICKEVENT_RECEIVESYSTEMTIME");

    /**
     * Team B pass statistics receive system time marker
     */
    private static final Marker BPassStatisticsReceiveSystemTimeMarker = MarkerFactory.getMarker("BPASSSTATISTICS_RECEIVESYSTEMTIME");

    /**
     * Pass sequence event receive system time marker
     */
    private static final Marker passSequenceEventReceiveSystemTimeMarker = MarkerFactory.getMarker("PASSSEQUENCEEVENT_RECEIVESYSTEMTIME");

    /**
     * Main method of the EvaluationConsumer.
     *
     * @param args Parameters
     */
    public static void main(String[] args) {
        String propertiesFilePath = "/evaluationConsumer.properties";

        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = EvaluationConsumer.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            logger.error("Unable to load {}", propertiesFilePath, e);
            System.exit(1);
        }

        logger.info("Read properties");
        long pollTimeout = PropertyReadHelper.readLongOrDie(properties, "kafka.pollTimeout");
        String brokerList = PropertyReadHelper.readStringOrDie(properties, "kafka.brokerList");
        String groupIdPrefix = PropertyReadHelper.readStringOrDie(properties, "kafka.groupIdPrefix");

        logger.info("Initializing EvaluationConsumer");
        // https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupIdPrefix + "_" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);

        List<String> topicsToSubscribe = new LinkedList<>();
        topicsToSubscribe.add("fieldObjectState");
        topicsToSubscribe.add("heatmapStatistics");
        topicsToSubscribe.add("kickEvent");
        topicsToSubscribe.add("passStatistics");
        topicsToSubscribe.add("passSequenceEvent");
        kafkaConsumer.subscribe(topicsToSubscribe);

        // Marker Hierarchy
        ballObjectStateReceiveSystemTimeMarker.add(parentReceiveSystemTimeMarker);
        A1FullGameHeatmapStatisticsReceiveSystemTimeMarker.add(parentReceiveSystemTimeMarker);
        kickEventReceiveSystemTimeMarker.add(parentReceiveSystemTimeMarker);
        BPassStatisticsReceiveSystemTimeMarker.add(parentReceiveSystemTimeMarker);
        passSequenceEventReceiveSystemTimeMarker.add(parentReceiveSystemTimeMarker);

        logger.info("Start consumption loop");
        boolean runFlag = true;
        while (runFlag) {
            try {
                ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(pollTimeout);
                for (ConsumerRecord<String, byte[]> record : records) {
                    String key = record.key();
                    Long sequenceNumber = record.offset();
                    byte[] contentByteArray = record.value();

                    try {
                        AbstractImmutableDataStreamElement dataStreamElement = AbstractImmutableDataStreamElement.generateDataStreamElementFromByteArray(key, contentByteArray, sequenceNumber, null, null);

                        if (!dataStreamElement.getStreamName().equals(record.topic())) {
                            logger.error("Cannot handle element ({}) since the stream name the data model assigns to the input stream element does not match the name of the Kafka topic via which it was received ({}).", dataStreamElement, record.topic());
                        } else {
                            Marker marker = null;
                            if (dataStreamElement.getStreamName().equals(FieldObjectStateStreamElement.STREAMNAME) && ((FieldObjectStateStreamElement) dataStreamElement).getObjectId().equals("BALL")) {
                                marker = ballObjectStateReceiveSystemTimeMarker;
                            } else if (dataStreamElement.getStreamName().equals(HeatmapStatisticsStreamElement.STREAMNAME) && !((HeatmapStatisticsStreamElement) dataStreamElement).isTeamStatistics() && ((HeatmapStatisticsStreamElement) dataStreamElement).getPlayerId().equals("A1") && ((HeatmapStatisticsStreamElement) dataStreamElement).getIntervalInS() == 0) {
                                marker = A1FullGameHeatmapStatisticsReceiveSystemTimeMarker;
                            } else if (dataStreamElement.getStreamName().equals(KickEventStreamElement.STREAMNAME)) {
                                marker = kickEventReceiveSystemTimeMarker;
                            } else if (dataStreamElement.getStreamName().equals(PassStatisticsStreamElement.STREAMNAME) && ((PassStatisticsStreamElement) dataStreamElement).isTeamStatistics() && ((PassStatisticsStreamElement) dataStreamElement).getTeamId().equals("B")) {
                                marker = BPassStatisticsReceiveSystemTimeMarker;
                            } else if (dataStreamElement.getStreamName().equals(PassSequenceEventStreamElement.STREAMNAME)) {
                                marker = passSequenceEventReceiveSystemTimeMarker;
                            }
                            if (marker != null) {
                                logger.info(marker, "{},{},{}", new Object[]{dataStreamElement.getKey(), dataStreamElement.getGenerationTimestamp(), System.currentTimeMillis()});
                            }
                        }
                    } catch (ClassNotFoundException | InvalidProtocolBufferException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                        logger.info("Caught exception during generating data stream element from byte array: ", e);
                    } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                        logger.info("Caught exception during checking data stream element for receive system time logging: ", e);
                    }
                }
            } catch (WakeupException e) {
                logger.info("Poll interrupted with wakeup call.");
            } catch (IllegalStateException e) {
                logger.error("Caught exception in main loop: ", e);
                try {
                    Thread.sleep(pollTimeout); // To prevent 100% CPU usage
                } catch (InterruptedException e2) {
                    logger.trace("InterruptedException in main loop.", e2);
                }
            }
        }

        kafkaConsumer.close();
        logger.info("Closed StreamConsumer");
    }
}
