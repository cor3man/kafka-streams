package org.example.kafkastreams;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class KafkaStreamsApplicationTests {

    @Test
    void contextLoads() {
    }

}
