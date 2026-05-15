package com.axiomstream.api.dto;


import com.axiomstream.core.model.EventRecord;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class EventGetResponse {

    private final int total;
    private final long nextOffset;
    private final List<EventRecord> events;

}
