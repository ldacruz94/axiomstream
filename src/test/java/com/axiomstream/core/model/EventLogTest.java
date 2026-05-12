package com.axiomstream.core.model;

import com.axiomstream.core.exceptions.EventSizeTooBigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventLogTest {

    @TempDir
    Path tempDir;

    @Test
    void append_shouldWriteEventToDiskAndAssignOffset() throws IOException {
        EventLog eventLog = new EventLog(
                "orders",
                0,
                tempDir.resolve("orders/partition-0"),
                new ArrayList<>(),
                null,
                0,
                1024 * 1024
        );

        EventRecord record = eventLog.append("order-42", "{\"status\":\"CREATED\"}");

        assertEquals(0, record.offset());
        assertEquals("order-42", record.key());
        assertEquals("{\"status\":\"CREATED\"}", record.payload());
        assertEquals(1, eventLog.getNextOffset());
        assertNotNull(eventLog.getActiveSegment());
        assertEquals(1, eventLog.getSegments().size());

        Path segmentFile = eventLog.getActiveSegment().getFilePath();

        assertTrue(Files.exists(segmentFile));

        List<String> lines = Files.readAllLines(segmentFile);

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("order-42"));
        assertTrue(lines.get(0).contains("{\"status\":\"CREATED\"}"));
    }

    @Test
    void append_shouldAssignSequentialOffsets() throws IOException {
        EventLog eventLog = new EventLog(
                "orders",
                0,
                tempDir.resolve("orders/partition-0"),
                new ArrayList<>(),
                null,
                0,
                1024 * 1024
        );

        EventRecord first = eventLog.append("order-1", "created");
        EventRecord second = eventLog.append("order-2", "paid");
        EventRecord third = eventLog.append("order-3", "shipped");

        assertEquals(0, first.offset());
        assertEquals(1, second.offset());
        assertEquals(2, third.offset());
        assertEquals(3, eventLog.getNextOffset());
    }

    @Test
    void readEvents_shouldReadEventsFromOffset() throws IOException {
        EventLog eventLog = new EventLog(
                "orders",
                0,
                tempDir.resolve("orders/partition-0"),
                new ArrayList<>(),
                null,
                0,
                1024 * 1024
        );

        eventLog.append("order-1", "created");
        eventLog.append("order-2", "paid");
        eventLog.append("order-3", "shipped");

        List<EventRecord> records = eventLog.readEvents(1, 2);

        assertEquals(2, records.size());
        assertEquals(1, records.get(0).offset());
        assertEquals("paid", records.get(0).payload());
        assertEquals(2, records.get(1).offset());
        assertEquals("shipped", records.get(1).payload());
    }

    @Test
    void append_shouldRotateSegmentsWhenSizeLimitReached() throws IOException {
        EventLog eventLog = new EventLog(
                "orders",
                0,
                tempDir.resolve("orders/partition-0"),
                new ArrayList<>(),
                null,
                0,
                80
        );

        eventLog.append("order-1", "this-is-a-somewhat-large-payload");
        eventLog.append("order-2", "this-is-another-somewhat-large-payload");
        eventLog.append("order-3", "this-is-yet-another-somewhat-large-payload");

        assertTrue(eventLog.getSegments().size() > 1);
        assertTrue(eventLog.getActiveSegment().isActive());

        long activeCount = eventLog.getSegments()
                .stream()
                .filter(LogSegment::isActive)
                .count();

        assertEquals(1, activeCount);
    }

    @Test
    void readEvents_shouldReadAcrossSegments() throws IOException {
        EventLog eventLog = new EventLog(
                "orders",
                0,
                tempDir.resolve("orders/partition-0"),
                new ArrayList<>(),
                null,
                0,
                80
        );

        eventLog.append("order-1", "created-created-created");
        eventLog.append("order-2", "paid-paid-paid");
        eventLog.append("order-3", "shipped-shipped-shipped");
        eventLog.append("order-4", "delivered-delivered-delivered");

        List<EventRecord> records = eventLog.readEvents(0, 4);

        assertEquals(4, records.size());
        assertEquals(0, records.get(0).offset());
        assertEquals(1, records.get(1).offset());
        assertEquals(2, records.get(2).offset());
        assertEquals(3, records.get(3).offset());
    }

    @Test
    void append_shouldRejectOversizedEvent() {
        EventLog eventLog = new EventLog(
                "orders",
                0,
                tempDir.resolve("orders/partition-0"),
                new ArrayList<>(),
                null,
                0,
                10
        );

        assertThrows(EventSizeTooBigException.class, () ->
                eventLog.append("order-1", "this-payload-is-way-too-large")
        );
    }
}