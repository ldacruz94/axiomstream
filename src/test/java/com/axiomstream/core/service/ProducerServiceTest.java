package com.axiomstream.core.service;

import com.axiomstream.api.dto.ProduceEventResponse;
import com.axiomstream.core.exceptions.TopicDoesNotExist;
import com.axiomstream.core.model.EventRecord;
import com.axiomstream.core.model.Partition;
import com.axiomstream.core.model.Topic;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProducerServiceTest {

    @Test
    void produce_shouldAppendEventToDeterministicPartition() throws IOException {
        TopicService topicService = mock(TopicService.class);

        Partition partition0 = mock(Partition.class);
        Partition partition1 = mock(Partition.class);
        Partition partition2 = mock(Partition.class);

        Topic topic = new Topic(
                "orders",
                3,
                List.of(partition0, partition1, partition2),
                Instant.now()
        );

        String key = "customer-123";
        String payload = "order-created";

        int expectedPartitionId = Math.floorMod(key.hashCode(), topic.getPartitionCount());

        EventRecord record = new EventRecord(
                42,
                key,
                payload,
                Instant.now(),
                payload.getBytes().length
        );

        when(topicService.getTopic("orders")).thenReturn(topic);
        when(topic.getPartitions().get(expectedPartitionId).appendEvent(key, payload)).thenReturn(record);

        ProducerService producerService = new ProducerService(topicService);

        ProduceEventResponse response = producerService.produce("orders", key, payload);

        assertEquals("orders", response.getTopic());
        assertEquals(key, response.getKey());
        assertEquals(expectedPartitionId, response.getPartitionId());
        assertEquals(42, response.getOffset());

        verify(topicService).getTopic("orders");
        verify(topic.getPartitions().get(expectedPartitionId)).appendEvent(key, payload);
    }

    @Test
    void produce_shouldThrowWhenTopicDoesNotExist() {
        TopicService topicService = mock(TopicService.class);

        when(topicService.getTopic("missing-topic"))
                .thenThrow(new NoSuchElementException("Topic does not exist"));

        ProducerService producerService = new ProducerService(topicService);

        assertThrows(
                TopicDoesNotExist.class,
                () -> producerService.produce("missing-topic", "key-1", "payload")
        );

        verify(topicService).getTopic("missing-topic");
    }

    @Test
    void produce_shouldThrowWhenTopicNameIsNull() {
        TopicService topicService = mock(TopicService.class);
        ProducerService producerService = new ProducerService(topicService);

        assertThrows(
                IllegalArgumentException.class,
                () -> producerService.produce(null, "key-1", "payload")
        );

        verifyNoInteractions(topicService);
    }

    @Test
    void produce_shouldThrowWhenKeyIsNull() {
        TopicService topicService = mock(TopicService.class);
        ProducerService producerService = new ProducerService(topicService);

        assertThrows(
                IllegalArgumentException.class,
                () -> producerService.produce("orders", null, "payload")
        );

        verifyNoInteractions(topicService);
    }

    @Test
    void produce_shouldThrowWhenPayloadIsNull() {
        TopicService topicService = mock(TopicService.class);
        ProducerService producerService = new ProducerService(topicService);

        assertThrows(
                IllegalArgumentException.class,
                () -> producerService.produce("orders", "key-1", null)
        );

        verifyNoInteractions(topicService);
    }

    @Test
    void sameKey_shouldRouteToSamePartition() throws IOException {
        TopicService topicService = mock(TopicService.class);

        Partition partition0 = mock(Partition.class);
        Partition partition1 = mock(Partition.class);
        Partition partition2 = mock(Partition.class);

        Topic topic = new Topic(
                "orders",
                3,
                List.of(partition0, partition1, partition2),
                Instant.now()
        );

        String key = "customer-123";
        int expectedPartitionId = Math.floorMod(key.hashCode(), topic.getPartitionCount());
        Partition expectedPartition = topic.getPartitions().get(expectedPartitionId);

        when(topicService.getTopic("orders")).thenReturn(topic);

        when(expectedPartition.appendEvent(eq(key), anyString()))
                .thenReturn(
                        new EventRecord(0, key, "event-1", Instant.now(), "event-1".getBytes().length),
                        new EventRecord(1, key, "event-2", Instant.now(), "event-2".getBytes().length)
                );

        ProducerService producerService = new ProducerService(topicService);

        ProduceEventResponse response1 = producerService.produce("orders", key, "event-1");
        ProduceEventResponse response2 = producerService.produce("orders", key, "event-2");

        assertEquals(response1.getPartitionId(), response2.getPartitionId());
        assertEquals(expectedPartitionId, response1.getPartitionId());
        assertEquals(expectedPartitionId, response2.getPartitionId());

        verify(expectedPartition, times(2)).appendEvent(eq(key), anyString());
    }
}