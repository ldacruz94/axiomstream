package com.axiomstream.core.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;


@Getter
@NonNull
@AllArgsConstructor
public class Partition {

    private int id;
    private String topicName;
    private EventLog eventLog;

    public EventRecord appendEvent(String key, String payload) throws IOException {
        return this.eventLog.append(key, payload);
    }

    public List<EventRecord> readEvents(long offset, int maxEvents) throws IOException {
        return this.eventLog.readEvents(offset, maxEvents);
    }

}
