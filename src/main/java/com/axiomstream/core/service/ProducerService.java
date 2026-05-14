package com.axiomstream.core.service;

import com.axiomstream.api.dto.ProduceEventResponse;
import com.axiomstream.core.exceptions.TopicDoesNotExist;
import com.axiomstream.core.model.EventRecord;
import com.axiomstream.core.model.Partition;
import com.axiomstream.core.model.Topic;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.NoSuchElementException;

@Service
public class ProducerService implements IProducerService {

    private final TopicService topicService;

    public ProducerService(TopicService topicService) {
        this.topicService = topicService;
    }

    public ProduceEventResponse produce(String topicName, String key, String payload) throws IOException {
        if (topicName == null || key == null || payload == null) {
            throw new IllegalArgumentException("Topic name, key, or payload cannot be null");
        }

        try {
            Topic topic = topicService.getTopic(topicName);

            int partitionCount = topic.getPartitionCount();
            int partitionId = Math.floorMod(key.hashCode(), partitionCount);

            Partition partition = topic.getPartitions().get(partitionId);
            EventRecord record = partition.appendEvent(key, payload);

            return new ProduceEventResponse(
                    topicName,
                    key,
                    partitionId,
                    record.offset()
            );
        } catch (NoSuchElementException ex) {
            throw new TopicDoesNotExist("Topic: " + topicName + " does not exist");
        }
    }
}
