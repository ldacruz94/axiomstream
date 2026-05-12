package com.axiomstream.core.model;

import java.time.Instant;

public record EventRecord(long offset, String key, String payload, Instant timeStamp, int sizeBytes)
{

    public String toLogLine() {
        return offset + "|" + timeStamp + "|" + key + "|" + payload;
    }

    public static EventRecord fromLogLine(String line) {
        String[] logLineSplit = line.split("\\|", 4); // avoids breaking from payloads with extra "|"
        return new EventRecord(
                Long.parseLong(logLineSplit[0]),
                logLineSplit[2],
                logLineSplit[3],
                Instant.parse(logLineSplit[1]),
                line.getBytes().length
        );
    }
}
