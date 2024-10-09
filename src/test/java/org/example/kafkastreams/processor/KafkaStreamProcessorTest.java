package org.example.kafkastreams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.example.kafkastreams.model.Client;
import org.example.kafkastreams.model.FraudClient;
import org.example.kafkastreams.model.Transaction;
import org.example.kafkastreams.factory.SerdeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class KafkaStreamProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Client> clientInputTopic;
    private TestInputTopic<String, Transaction> transactionInputTopic;
    private TestOutputTopic<Void, FraudClient> fraudOutputTopic;

    @BeforeEach
    public void setUp() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        SerdeFactory serdeFactory = new SerdeFactory(objectMapper);

        StreamsBuilder builder = new StreamsBuilder();
        KafkaStreamProcessor processor = new KafkaStreamProcessor(streamProperties, serdeFactory);
        processor.buildStream(builder);

        testDriver = new TopologyTestDriver(builder.build(), streamProperties);

        clientInputTopic = testDriver.createInputTopic("client-topic", Serdes.String().serializer(), serdeFactory.createSerde(Client.class).serializer());
        transactionInputTopic = testDriver.createInputTopic("transaction-topic", Serdes.String().serializer(), serdeFactory.createSerde(Transaction.class).serializer());
        fraudOutputTopic = testDriver.createOutputTopic("fraud-topic", Serdes.Void().deserializer(), serdeFactory.createSerde(FraudClient.class).deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void testFraudDetectionWhenClientNameLengthMoreThan8AndTotalTransactionsPriceMoreThan1000() {
        Client client = Client.builder()
                .clientId(11L)
                .lastName("long_name")
                .build();

        Transaction transaction1 = Transaction.builder()
                .clientId(11L)
                .price(500.0)
                .build();
        Transaction transaction2 = Transaction.builder()
                .clientId(11L)
                .price(501.0)
                .build();

        clientInputTopic.pipeInput("clientId1", client);
        transactionInputTopic.pipeInput("clientId1", transaction1);
        transactionInputTopic.pipeInput("clientId1", transaction2);

        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        KeyValue<Void, FraudClient> result = fraudOutputTopic.readKeyValue();
        assertEquals(11L, result.value.getClient().getClientId());
        assertEquals(1001.0, result.value.getTotalAmount());
    }

    @Test
    public void testFraudDetectionWhenClientNameLengthLessThan8AndTotalTransactionsPriceMoreThan1000() {
        Client client = Client.builder()
                .clientId(11L)
                .lastName("name")
                .build();

        Transaction transaction1 = Transaction.builder()
                .clientId(11L)
                .price(500.0)
                .build();
        Transaction transaction2 = Transaction.builder()
                .clientId(11L)
                .price(501.0)
                .build();

        clientInputTopic.pipeInput("clientId1", client);
        transactionInputTopic.pipeInput("clientId1", transaction1);
        transactionInputTopic.pipeInput("clientId1", transaction2);

        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        assertTrue(fraudOutputTopic.isEmpty());
    }

    @Test
    public void testFraudDetectionWhenClientNameLengthMoreThan8AndTotalTransactionsPricelessThan1000() {
        Client client = Client.builder()
                .clientId(11L)
                .lastName("long_name")
                .build();

        Transaction transaction1 = Transaction.builder()
                .clientId(11L)
                .price(50.0)
                .build();
        Transaction transaction2 = Transaction.builder()
                .clientId(11L)
                .price(501.0)
                .build();

        clientInputTopic.pipeInput("clientId1", client);
        transactionInputTopic.pipeInput("clientId1", transaction1);
        transactionInputTopic.pipeInput("clientId1", transaction2);

        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        assertTrue(fraudOutputTopic.isEmpty());
    }
}
