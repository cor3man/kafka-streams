package org.example.kafkastreams.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.example.kafkastreams.factory.SerdeFactory;
import org.example.kafkastreams.model.Client;
import org.example.kafkastreams.model.EnrichedTransaction;
import org.example.kafkastreams.model.FraudClient;
import org.example.kafkastreams.model.Transaction;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Properties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class KafkaStreamProcessor {

    private final Properties streamProperties;
    private final SerdeFactory serdeFactory;
    private KafkaStreams kafkaStreams;

    public KafkaStreamProcessor(final Properties streamProperties, final SerdeFactory serdeFactory) {
        this.streamProperties = streamProperties;
        this.serdeFactory = serdeFactory;
    }

    @PostConstruct
    public void startKafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();
        buildStream(builder);

        kafkaStreams = new KafkaStreams(builder.build(), streamProperties);
        kafkaStreams.start();
    }

    @PreDestroy
    public void stopKafkaStreams() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
    }

    public void buildStream(StreamsBuilder builder) {
        final Serde<Client> clientSerde = serdeFactory.createSerde(Client.class);
        final Serde<Transaction> transactionSerde = serdeFactory.createSerde(Transaction.class);
        final Serde<EnrichedTransaction> enrichedTransactionSerde = serdeFactory.createSerde(EnrichedTransaction.class);
        final Serde<FraudClient> fraudClientSerde = serdeFactory.createSerde(FraudClient.class);

        KStream<String, Client> clientStream =
                builder.stream("client-topic", Consumed.with(Serdes.String(), clientSerde))
                        .selectKey((key, client) -> client.getClientId().toString());

        KStream<String, Transaction> transactionStream =
                builder.stream("transaction-topic", Consumed.with(Serdes.String(), transactionSerde))
                        .selectKey((key, transaction) -> transaction.getClientId().toString());

        KStream<String, EnrichedTransaction> joinedStream = transactionStream.join(clientStream,
                (transaction, client) -> new EnrichedTransaction(client, transaction),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)),
                StreamJoined.with(Serdes.String(), transactionSerde, clientSerde));

        KTable<Windowed<Client>, Double> aggregatedTransactions =
                joinedStream.groupBy((key, value) -> value.getClient(),
                                Grouped.with(clientSerde, enrichedTransactionSerde))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2))).aggregate(() -> 0.0,
                                (client, enrichedTransaction, totalSum) -> totalSum + enrichedTransaction.getTransaction()
                                        .getPrice(),
                                Materialized.<Client, Double, WindowStore<Bytes, byte[]>>as("transaction-sum-store")
                                        .withValueSerde(Serdes.Double()));

        aggregatedTransactions.toStream()
                .filter((client, amount) -> client.key().getLastName().length() > 8 && amount > 1000)
                .map((key, value) -> new KeyValue<>(null, new FraudClient(key.key(), value)))
                .to("fraud-topic", Produced.valueSerde(fraudClientSerde));

    }
}
