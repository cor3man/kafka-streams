package org.example.kafkastreams.model;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {

    private Long transactionId;
    private Long clientId;
    private String bank;
    private TransactionType orderType;
    private Integer quantity;
    private Double price;
    private Double cost;
    private LocalDateTime createdAt;
}


