package org.example.kafkastreams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor(force = true)
@AllArgsConstructor
public class EnrichedTransaction {
    private final Client client;
    private final Transaction transaction;
}
