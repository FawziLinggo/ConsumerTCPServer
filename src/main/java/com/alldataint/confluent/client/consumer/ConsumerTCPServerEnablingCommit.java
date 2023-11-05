package com.alldataint.confluent.client.consumer;

import com.alldataint.confluent.client.consumer.constants.CommonConstants;
import jdk.net.ExtendedSocketOptions;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ConsumerTCPServerEnablingCommit {
  private static final Logger logger = LogManager.getLogger(ConsumerTCPServerEnablingCommit.class.getName());

  public static void main(String[] args) throws IOException {

    InputStream inputStream = Files.newInputStream(Paths.get("src/main/resources/config.properties"));

    Properties props = new Properties();
    props.load(inputStream);

    String topicName = props.getProperty("topic.name");
    int port = props.getProperty("tcp.port") != null ? Integer.parseInt(props.getProperty("tcp.port")) : 11001;
    ServerSocket server = new ServerSocket(port);



    try (KafkaConsumer < String, String > consumer = new KafkaConsumer < > (props)) {
      consumer.subscribe(List.of(topicName));

      //polling
      while (true) {
        Socket socket = server.accept();
        socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, 10);
        socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, 2);
        socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, 3);
        socket.setKeepAlive(true);
        socket.setSoTimeout(1000);


        if (socket.isConnected()) {

          while (true) {
            ConsumerRecords < String, String > records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord < String, String > record: records) {
              String kafkaMessage = record.value();
              System.out.println(record.value());

              PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
              out.println("Pesan dari Kafka: " + kafkaMessage);


            }
            // TCP keep alive




            // TODO : Fixing the ping-pong mechanism or another mechanism to check the connection
            String time = java.time.LocalTime.now().toString();
              // check every 30 second
                if (time.startsWith("00", 6) || time.startsWith("30", 6)) {
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(CommonConstants.ping);
                    System.out.println("Data dikirim ke klien.");

                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String clientData = reader.readLine();

                  System.out.println("Client data: " + clientData);
                  System.out.println("Client data: " + clientData == null);

                    int length = clientData.length();
                    System.out.println("Client data length: " + length);


                    if (Objects.equals(clientData, CommonConstants.pong)) {
                    System.out.println("Klien mengirimkan data: " + clientData);
                    }else if (clientData== null) {
                        System.out.println("Klien tidak mengirimkan data.");
                        consumer.close();
                        socket.close();
                    }
                }

          }

        }
        consumer.close();
      }
    }
  }
}