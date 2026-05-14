package com.axiomstream.api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ProduceEventResponse {
    private String topic;
    private String key;
    private int partitionId;
    private long offset;
}