package com.axiomstream.core.model;

import com.axiomstream.core.exceptions.EventSizeTooBigException;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Getter
@AllArgsConstructor
public class EventLog {

    private String topicName;
    private int partitionId;
    private Path logDirectory;
    private List<LogSegment> segments;
    private LogSegment activeSegment;
    private long nextOffset;
    private long maxSegmentSizeBytes;

    public EventRecord append(String key, String payload) throws IOException {
        Files.createDirectories(logDirectory);

        EventRecord eventRecord = new EventRecord(
                this.nextOffset,
                key,
                payload,
                Instant.now(),
                payload.getBytes().length
        );

        if (eventRecord.sizeBytes() >= maxSegmentSizeBytes) {
            throw new EventSizeTooBigException("The EventRecord provided is too big");
        }

        if (activeSegment == null) {
            activeSegment = new LogSegment(
                    logDirectory.resolve(nextOffset + ".log"),
                    nextOffset,
                    nextOffset,
                    0,
                    true
            );
            segments.add(activeSegment);
        }

        // check if the new sizeBytes will reach the segment size cap
        if (activeSegment.getCurrentSizeBytes() + eventRecord.sizeBytes() >= maxSegmentSizeBytes) {
            this.rotateSegment();
        }

        activeSegment.append(eventRecord);
        nextOffset++;

        return eventRecord;
    }

    public List<EventRecord> readEvents(long offset, int maxEvents) throws IOException {
        List<EventRecord> records = new ArrayList<>();

        for (var segment : segments) {

            if (records.size() >= maxEvents) {
                break;
            }

            if (segment.getBaseOffset() <= offset && offset < segment.getNextOffset()) {
                int remaining = maxEvents - records.size();
                records.addAll(segment.readFrom(offset, remaining));
            }

            else if (segment.getBaseOffset() > offset) {
                int remaining = maxEvents - records.size();
                records.addAll(
                        segment.readFrom(segment.getBaseOffset(), remaining)
                );
            }
        }

        return records;
    }

    public void rotateSegment() {
        this.activeSegment.setActive(false);
        long baseOffset = nextOffset;

        activeSegment = new LogSegment(
                logDirectory.resolve(nextOffset + ".log"),
                baseOffset,
                baseOffset,
                0,
                true
        );

        segments.add(activeSegment);
    }
}
