package com.alldataint.confluent.client.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerTCPServer {
    private static final Logger logger = LogManager.getLogger(ConsumerTCPServer.class.getName());

    public static void main(String[] args) {
        try {
            Properties kafkaProperties = loadKafkaProperties();
            subscribeToKafkaTopic(kafkaProperties);
        } catch (IOException e) {
            handleException(e);
        }
    }

    private static Properties loadKafkaProperties() throws IOException {
        try (InputStream inputStream = new FileInputStream("src/main/resources/config.properties")) {
            Properties props = new Properties();
//            props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
            props.load(inputStream);
            return props;
        }
    }

    private static void subscribeToKafkaTopic(Properties kafkaProperties) {
        String topicName = "attrep_status";
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(kafkaProperties)) {
            consumer.subscribe(List.of(topicName));
            // Your consumer logic here
            while(true){
            ConsumerRecords<String,GenericRecord> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,GenericRecord> record: records){
                System.out.println(record.value());
            }
        }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private static void handleException(Exception e) {
        logger.error(e);
        throw new RuntimeException(e);
    }
}