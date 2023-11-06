package com.alldataint.confluent.client.consumer;


import com.alldataint.confluent.client.consumer.constants.CommonConstants;

import java.io.*;
import java.net.*;
import java.util.Objects;

public class TesttingTCPClient {

    public static void main(String[] args){
        // create simple TCP client and connect to server
        try {
            Socket cs=new Socket("localhost",11002);
            System.out.println("Connected to Kafka topic: " + cs.isConnected());

            while(cs.isConnected()){

                // read message from server
                BufferedReader br=new BufferedReader(new InputStreamReader(cs.getInputStream()));
                String msg=br.readLine();
                System.out.println(msg);

                if (Objects.equals(msg, CommonConstants.ping)) {
                    PrintWriter pw=new PrintWriter(cs.getOutputStream(),true);
                    pw.println(CommonConstants.pong);
                }

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
