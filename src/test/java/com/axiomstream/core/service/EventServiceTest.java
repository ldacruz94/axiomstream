package com.axiomstream.core.service;

import com.axiomstream.api.dto.EventGetRequest;
import com.axiomstream.api.dto.EventGetResponse;
import com.axiomstream.core.model.EventLog;
import com.axiomstream.core.model.EventRecord;
import com.axiomstream.core.model.Partition;
import com.axiomstream.core.model.Topic;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventServiceTest {

    @Test
    void getEvents_shouldReturnEventsFromRequestedPartition() throws IOException {
        TopicService topicService = mock(TopicService.class);
        Topic topic = mock(Topic.class);
        Partition partition = mock(Partition.class);
        EventLog eventLog = mock(EventLog.class);

        EventService eventService = new EventService(topicService);

        EventGetRequest request = new EventGetRequest(
                "orders",
                0,
                0L,
                10
        );

        List<EventRecord> events = List.of(
                new EventRecord(0L, "key-1", "payload-1", Instant.now(), 0),
                new EventRecord(1L, "key-2", "payload-2", Instant.now(),0)
        );

        when(topicService.getTopic("orders")).thenReturn(topic);
        when(topic.getPartitions()).thenReturn(List.of(partition));
        when(partition.readEvents(0L, 10)).thenReturn(events);
        when(partition.getEventLog()).thenReturn(eventLog);
        when(eventLog.getNextOffset()).thenReturn(2L);

        EventGetResponse response = eventService.getEvents(request);

        assertEquals(2, response.getTotal());
        assertEquals(2L, response.getNextOffset());
        assertEquals(events, response.getEvents());

        verify(topicService).getTopic("orders");
        verify(topic).getPartitions();
        verify(partition).readEvents(0L, 10);
        verify(partition).getEventLog();
        verify(eventLog).getNextOffset();
    }

    @Test
    void getEvents_shouldReadFromRequestedPartitionId() throws IOException {
        TopicService topicService = mock(TopicService.class);
        Topic topic = mock(Topic.class);
        Partition partition0 = mock(Partition.class);
        Partition partition1 = mock(Partition.class);
        EventLog eventLog = mock(EventLog.class);

        EventService eventService = new EventService(topicService);

        EventGetRequest request = new EventGetRequest(
                "orders",
                1,
                5L,
                10
        );

        List<EventRecord> events = List.of(
                new EventRecord(5L, "key-1", "payload-1", Instant.now(), 0)
        );

        when(topicService.getTopic("orders")).thenReturn(topic);
        when(topic.getPartitions()).thenReturn(List.of(partition0, partition1));
        when(partition1.readEvents(5L, 10)).thenReturn(events);
        when(partition1.getEventLog()).thenReturn(eventLog);
        when(eventLog.getNextOffset()).thenReturn(6L);

        EventGetResponse response = eventService.getEvents(request);

        assertEquals(1, response.getTotal());
        assertEquals(6L, response.getNextOffset());
        assertEquals(events, response.getEvents());

        verify(partition1).readEvents(5L, 10);
        verify(partition0, never()).readEvents(anyLong(), anyInt());
    }

    @Test
    void getEvents_shouldReturnEmptyResponseWhenNoEventsFound() throws IOException {
        TopicService topicService = mock(TopicService.class);
        Topic topic = mock(Topic.class);
        Partition partition = mock(Partition.class);
        EventLog eventLog = mock(EventLog.class);

        EventService eventService = new EventService(topicService);

        EventGetRequest request = new EventGetRequest(
                "orders",
                0,
                10L,
                10
        );

        when(topicService.getTopic("orders")).thenReturn(topic);
        when(topic.getPartitions()).thenReturn(List.of(partition));
        when(partition.readEvents(10L, 10)).thenReturn(List.of());
        when(partition.getEventLog()).thenReturn(eventLog);
        when(eventLog.getNextOffset()).thenReturn(10L);

        EventGetResponse response = eventService.getEvents(request);

        assertEquals(0, response.getTotal());
        assertEquals(10L, response.getNextOffset());
        assertEquals(List.of(), response.getEvents());
    }

    @Test
    void getEvents_shouldThrowIOExceptionWhenPartitionReadFails() throws IOException {
        TopicService topicService = mock(TopicService.class);
        Topic topic = mock(Topic.class);
        Partition partition = mock(Partition.class);

        EventService eventService = new EventService(topicService);

        EventGetRequest request = new EventGetRequest(
                "orders",
                0,
                0L,
                10
        );

        when(topicService.getTopic("orders")).thenReturn(topic);
        when(topic.getPartitions()).thenReturn(List.of(partition));
        when(partition.readEvents(0L, 10))
                .thenThrow(new IOException("Failed to read log"));

        assertThrows(IOException.class, () -> eventService.getEvents(request));

        verify(partition).readEvents(0L, 10);
    }

    @Test
    void getEvents_shouldThrowWhenPartitionIdDoesNotExist() {
        TopicService topicService = mock(TopicService.class);
        Topic topic = mock(Topic.class);

        EventService eventService = new EventService(topicService);

        EventGetRequest request = new EventGetRequest(
                "orders",
                5,
                0L,
                10
        );

        when(topicService.getTopic("orders")).thenReturn(topic);
        when(topic.getPartitions()).thenReturn(List.of());

        assertThrows(IndexOutOfBoundsException.class,
                () -> eventService.getEvents(request));
    }
}