package com.axiomstream.core.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class LogSegment {

    private Path filePath;
    private long baseOffset;
    private long nextOffset;
    private long currentSizeBytes;
    private boolean active;

    public void append(EventRecord eventRecord) throws IOException {
        Files.writeString(
                filePath,
                eventRecord.toLogLine() + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );

        currentSizeBytes += eventRecord.sizeBytes();
        nextOffset++;
    }

    public List<EventRecord> readFrom(long offset, int maxEvents) throws IOException {
        List<String> lines = Files.readAllLines(filePath);
        List<EventRecord> records = new ArrayList<>();
        int startingIndex = (int)(offset - baseOffset);

        if (startingIndex < 0) {
            throw new IOException("Starting index must be equal or greater than 0");
        }

        if (startingIndex >= lines.size()) {
            return records;
        }

        int cap = Math.min(startingIndex + maxEvents, lines.size()); // in case line quantity is smaller
        for (int i = startingIndex; i < cap; i++) {
            records.add(EventRecord.fromLogLine(lines.get(i)));
        }

        return records;
    }
}
