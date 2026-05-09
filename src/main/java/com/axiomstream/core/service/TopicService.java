package com.axiomstream.core.service;

import com.axiomstream.core.exceptions.PartitionCountIsZeroException;
import com.axiomstream.core.exceptions.TopicAlreadyExistsException;
import com.axiomstream.core.model.Partition;
import com.axiomstream.core.model.Topic;
import com.axiomstream.core.registry.TopicRegistry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TopicService implements ITopicService {

    private final TopicRegistry topicRegistry;

    public TopicService(TopicRegistry topicRegistry) {
        this.topicRegistry = topicRegistry;
    }

    public void createTopic(String name, int partitionCount) {

        if (partitionCount == 0) {
            throw new PartitionCountIsZeroException("Partition count needs to be greater than 0");
        }

        if (topicRegistry.getTopic(name) != null) {
            throw new TopicAlreadyExistsException("Topic name already exists: " + name);
        }

        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new Partition(i));
        }

        Topic topic = new Topic(name, partitionCount, partitions, Instant.now());
        topicRegistry.createTopic(topic);
    }

    public Topic getTopic(String name) {
        return topicRegistry.getTopic(name);
    }

    public List<Topic> listTopics() {
        return topicRegistry.listTopics();
    }
}
