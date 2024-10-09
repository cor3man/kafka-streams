package org.example.kafkastreams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Client {
    private Long clientId;
    private String email;
    private String firstName;
    private String lastName;

}
