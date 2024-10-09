package org.example.kafkastreams;

import org.springframework.boot.SpringApplication;

public class TestKafkaStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.from(KafkaStreamsApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
