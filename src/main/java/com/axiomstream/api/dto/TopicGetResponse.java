package com.axiomstream.api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicGetResponse {

    private String name;
    private int partitions;

}
