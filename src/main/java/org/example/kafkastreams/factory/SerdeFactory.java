package org.example.kafkastreams.factory;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class SerdeFactory {

    private final ObjectMapper objectMapper;

    public <T> Serde<T> createSerde(Class<T> clazz) {

        Serializer<T> serializer = (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing object", e);
            }
        };
        Deserializer<T> deserializer = (topic, data) -> {
            try {
                return objectMapper.readValue(data, clazz);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing object", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }

}


