package com.axiomstream.api.dto;

import lombok.Data;

@Data
public class TopicCreateRequest {

    private String name;
    private int partitions;

}
