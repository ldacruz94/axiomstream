package com.axiomstream.core.service;

import com.axiomstream.api.dto.OffsetCommitRequest;
import com.axiomstream.api.dto.OffsetCommitResponse;
import com.axiomstream.api.dto.OffsetGetRequest;
import com.axiomstream.api.dto.OffsetGetResponse;
import com.axiomstream.core.model.ConsumerOffsetKey;
import com.axiomstream.core.model.Topic;
import com.axiomstream.core.store.ConsumerOffsetStore;
import org.springframework.stereotype.Service;

@Service
public class ConsumerOffsetService implements IConsumerOffsetService {

    private final ITopicService topicService;
    private final ConsumerOffsetStore offsetStore;

    public ConsumerOffsetService(ITopicService topicService, ConsumerOffsetStore offsetStore) {
        this.topicService = topicService;
        this.offsetStore = offsetStore;
    }

    public OffsetGetResponse getOffset(OffsetGetRequest request) {
        validateGetRequest(request);

        Topic topic = topicService.getTopic(request.getTopicName());
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + request.getTopicName());
        }

        ConsumerOffsetKey key = new ConsumerOffsetKey(request.getConsumerId(), request.getTopicName(), request.getPartitionId());

        try {
            long offset = offsetStore.get(key);
            return new OffsetGetResponse(request.getConsumerId(), request.getTopicName(), request.getPartitionId(), offset);
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve offset for consumer: " + request.getConsumerId() + ", topic: " + request.getTopicName() + ", partition: " + request.getPartitionId(), e);
        }
    }

    public OffsetCommitResponse commitOffset(OffsetCommitRequest request) {
        validateCommitRequest(request);

        try {
            ConsumerOffsetKey key = new ConsumerOffsetKey(request.getConsumerId(), request.getTopicName(), request.getPartitionId());
            offsetStore.put(key, request.getOffset());
            return new OffsetCommitResponse(
                    request.getConsumerId(), request.getTopicName(), request.getPartitionId(), request.getOffset(), true
            );
        } catch (Exception e) {
            return new OffsetCommitResponse(
                    request.getConsumerId(), request.getTopicName(), request.getPartitionId(), request.getOffset(), false
            );
        }
    }

    private void validateGetRequest(OffsetGetRequest request) {
        validateRequest(request.getConsumerId(), request.getTopicName(), request.getPartitionId());
    }

    private void validateCommitRequest(OffsetCommitRequest request) {
        validateRequest(request.getConsumerId(), request.getTopicName(), request.getPartitionId());
    }

    private void validateRequest(String consumerId, String topicName, int partitionId) {
        if (consumerId == null || consumerId.isEmpty() || topicName == null || topicName.isEmpty() || partitionId < 0) {
            throw new IllegalArgumentException("Consumer ID, topic name, and partition ID cannot be null or invalid");
        }

        Topic topic = this.topicService.getTopic(topicName);

        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }

        if (topic.getPartitions().get(partitionId) == null) {
            throw new IllegalArgumentException("Partition does not exist: " + partitionId);
        }
    }
}
