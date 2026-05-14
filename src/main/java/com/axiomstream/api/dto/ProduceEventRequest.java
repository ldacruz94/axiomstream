package com.axiomstream.api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ProduceEventRequest {
    private String key;
    private String payload;
}
