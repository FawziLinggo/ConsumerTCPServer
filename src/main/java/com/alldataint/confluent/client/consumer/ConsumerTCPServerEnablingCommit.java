package com.alldataint.confluent.client.consumer;

import com.alldataint.confluent.client.consumer.constants.CommonConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class ConsumerTCPServerEnablingCommit {
  private static final Logger logger = LogManager.getLogger(ConsumerTCPServerEnablingCommit.class.getName());

  public static void main(String[] args) throws IOException {

    InputStream inputStream = Files.newInputStream(Paths.get("src/main/resources/config.properties"));

    Properties props = new Properties();
    props.load(inputStream);

    String topicName = props.getProperty("topic.name");
    int port = props.getProperty("tcp.port") != null ? Integer.parseInt(props.getProperty("tcp.port")) : 11001;
    ServerSocket server = new ServerSocket(port);


      //polling
      while (true) {
        try (KafkaConsumer < String, String > consumer = new KafkaConsumer < > (props)) {
          consumer.subscribe(List.of(topicName));
            Socket socket = server.accept();
            socket.setKeepAlive(true);
            socket.setSoTimeout(1000);


            if (socket.isConnected()) {

              while (true) {
                ConsumerRecords < String, String > records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord < String, String > record: records) {
                  String kafkaMessage = record.value();
                  PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                  out.println(kafkaMessage);
                  consumer.commitSync();
                }

                // TCP keep alive
                // TODO : Fixing the ping-pong mechanism or another mechanism to check the connection
                String time = java.time.LocalTime.now().toString();
                  // check every 60 second
                    if (time.startsWith("00", 6)) {
                        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                        writer.println(CommonConstants.ping);

                        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        try {
                            String clientData = reader.readLine();
                            if (Objects.equals(clientData, CommonConstants.pong)) {
                                logger.info("Last offset committed: " + consumer.committed(consumer.assignment()));
                            }
                        } catch (IOException e) {
                            logger.error("Error while reading data from client: " + e.getMessage());
                            consumer.close();
                            socket.close();
                            break;
                        }
                    }
              }
            }
          }
    }
  }
}