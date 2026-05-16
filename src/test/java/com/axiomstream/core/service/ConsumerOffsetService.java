package com.axiomstream.core.service;

import com.axiomstream.api.dto.OffsetCommitRequest;
import com.axiomstream.api.dto.OffsetCommitResponse;
import com.axiomstream.api.dto.OffsetGetRequest;
import com.axiomstream.api.dto.OffsetGetResponse;
import com.axiomstream.core.model.ConsumerOffsetKey;
import com.axiomstream.core.model.Partition;
import com.axiomstream.core.model.Topic;
import com.axiomstream.core.store.ConsumerOffsetStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsumerOffsetServiceTest {

    private ITopicService topicService;
    private ConsumerOffsetStore offsetStore;
    private ConsumerOffsetService consumerOffsetService;

    @BeforeEach
    void setUp() {
        topicService = mock(ITopicService.class);
        offsetStore = mock(ConsumerOffsetStore.class);
        consumerOffsetService = new ConsumerOffsetService(topicService, offsetStore);
    }

    @Test
    void commitOffset_shouldStoreOffsetAndReturnSuccessResponse() {
        Topic topic = mockTopicWithPartitions(2);
        when(topicService.getTopic("orders")).thenReturn(topic);

        OffsetCommitRequest request = new OffsetCommitRequest(
                "worker-1",
                "orders",
                0,
                5L
        );

        OffsetCommitResponse response = consumerOffsetService.commitOffset(request);

        assertEquals("worker-1", response.getConsumerId());
        assertEquals("orders", response.getTopicName());
        assertEquals(0, response.getPartitionId());
        assertEquals(5L, response.getOffset());
        assertTrue(response.isSuccess());

        ArgumentCaptor<ConsumerOffsetKey> keyCaptor = ArgumentCaptor.forClass(ConsumerOffsetKey.class);
        verify(offsetStore).put(keyCaptor.capture(), eq(5L));

        ConsumerOffsetKey capturedKey = keyCaptor.getValue();
        assertEquals("worker-1", capturedKey.getConsumerId());
        assertEquals("orders", capturedKey.getTopicName());
        assertEquals(0, capturedKey.getPartitionId());
    }

    @Test
    void getOffset_shouldReturnStoredOffset() {
        Topic topic = mockTopicWithPartitions(2);
        when(topicService.getTopic("orders")).thenReturn(topic);
        when(offsetStore.get(any(ConsumerOffsetKey.class))).thenReturn(5L);

        OffsetGetRequest request = new OffsetGetRequest(
                "worker-1",
                "orders",
                0
        );

        OffsetGetResponse response = consumerOffsetService.getOffset(request);

        assertEquals("worker-1", response.getConsumerId());
        assertEquals("orders", response.getTopicName());
        assertEquals(0, response.getPartitionId());
        assertEquals(5L, response.getOffset());

        verify(offsetStore).get(any(ConsumerOffsetKey.class));
    }

    @Test
    void getOffset_shouldReturnZeroWhenNoOffsetExists() {
        Topic topic = mockTopicWithPartitions(2);
        when(topicService.getTopic("orders")).thenReturn(topic);
        when(offsetStore.get(any(ConsumerOffsetKey.class))).thenReturn(0L);

        OffsetGetRequest request = new OffsetGetRequest(
                "worker-1",
                "orders",
                0
        );

        OffsetGetResponse response = consumerOffsetService.getOffset(request);

        assertEquals(0L, response.getOffset());
    }

    @Test
    void commitOffset_shouldThrowExceptionWhenConsumerIdIsNull() {
        OffsetCommitRequest request = new OffsetCommitRequest(
                null,
                "orders",
                0,
                5L
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.commitOffset(request)
        );

        verifyNoInteractions(offsetStore);
    }

    @Test
    void commitOffset_shouldThrowExceptionWhenConsumerIdIsEmpty() {
        OffsetCommitRequest request = new OffsetCommitRequest(
                "",
                "orders",
                0,
                5L
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.commitOffset(request)
        );

        verifyNoInteractions(offsetStore);
    }

    @Test
    void commitOffset_shouldThrowExceptionWhenTopicNameIsNull() {
        OffsetCommitRequest request = new OffsetCommitRequest(
                "worker-1",
                null,
                0,
                5L
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.commitOffset(request)
        );

        verifyNoInteractions(offsetStore);
    }

    @Test
    void commitOffset_shouldThrowExceptionWhenTopicNameIsEmpty() {
        OffsetCommitRequest request = new OffsetCommitRequest(
                "worker-1",
                "",
                0,
                5L
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.commitOffset(request)
        );

        verifyNoInteractions(offsetStore);
    }

    @Test
    void commitOffset_shouldThrowExceptionWhenPartitionIdIsNegative() {
        OffsetCommitRequest request = new OffsetCommitRequest(
                "worker-1",
                "orders",
                -1,
                5L
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.commitOffset(request)
        );

        verifyNoInteractions(offsetStore);
    }

    @Test
    void commitOffset_shouldThrowExceptionWhenTopicDoesNotExist() {
        when(topicService.getTopic("orders")).thenReturn(null);

        OffsetCommitRequest request = new OffsetCommitRequest(
                "worker-1",
                "orders",
                0,
                5L
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.commitOffset(request)
        );

        verify(offsetStore, never()).put(any(), anyLong());
    }

    @Test
    void getOffset_shouldThrowExceptionWhenTopicDoesNotExist() {
        when(topicService.getTopic("orders")).thenReturn(null);

        OffsetGetRequest request = new OffsetGetRequest(
                "worker-1",
                "orders",
                0
        );

        assertThrows(IllegalArgumentException.class, () ->
                consumerOffsetService.getOffset(request)
        );

        verify(offsetStore, never()).get(any());
    }

    @Test
    void getOffset_shouldThrowExceptionWhenPartitionDoesNotExist() {
        Topic topic = mockTopicWithPartitions(1);
        when(topicService.getTopic("orders")).thenReturn(topic);

        OffsetGetRequest request = new OffsetGetRequest(
                "worker-1",
                "orders",
                5
        );

        assertThrows(RuntimeException.class, () ->
                consumerOffsetService.getOffset(request)
        );

        verify(offsetStore, never()).get(any());
    }

    @Test
    void commitOffset_shouldReturnFailureResponseWhenStoreThrowsException() {
        Topic topic = mockTopicWithPartitions(2);
        when(topicService.getTopic("orders")).thenReturn(topic);

        doThrow(new RuntimeException("Store failed"))
                .when(offsetStore)
                .put(any(ConsumerOffsetKey.class), eq(5L));

        OffsetCommitRequest request = new OffsetCommitRequest(
                "worker-1",
                "orders",
                0,
                5L
        );

        OffsetCommitResponse response = consumerOffsetService.commitOffset(request);

        assertFalse(response.isSuccess());
        assertEquals(5L, response.getOffset());
    }

    private Topic mockTopicWithPartitions(int partitionCount) {
        Topic topic = mock(Topic.class);

        List<Partition> partitions = mock(List.class);

        when(partitions.size()).thenReturn(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            when(partitions.get(i)).thenReturn(mock(Partition.class));
        }

        when(topic.getPartitions()).thenReturn(partitions);

        return topic;
    }
}