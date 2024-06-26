package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No file is provided!");
            System.exit(-1);
            return;
        }

        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("File not found!");
            System.exit(-1);
            return;
        }

        Properties config=new Properties();
        config.put("bootstrap.servers","localhost:9092");
        config.put("")

        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                reader.lines().forEach(System.out::println);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
