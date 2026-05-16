package com.axiomstream.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConsumerOffsetKey {

    private String consumerId;
    private String topicName;
    private int partitionId;

}
