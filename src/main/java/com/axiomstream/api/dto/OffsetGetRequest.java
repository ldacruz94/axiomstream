package com.axiomstream.api.dto;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OffsetGetRequest {

    private String consumerId;
    private String topicName;
    private int partitionId;

}
