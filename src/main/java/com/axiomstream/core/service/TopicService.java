package com.axiomstream.core.service;

import com.axiomstream.core.exceptions.PartitionCountIsZeroException;
import com.axiomstream.core.exceptions.TopicAlreadyExistsException;
import com.axiomstream.core.model.*;
import com.axiomstream.core.registry.TopicRegistry;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TopicService implements ITopicService {

    private final TopicRegistry topicRegistry;
    private final EventLogConfig eventLogConfig = this.setupEventLogConfig();

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
            Partition partition = new Partition(i, name,
                    new EventLog(
                            name,
                            i,
                            this.eventLogConfig.resolvePartitionDirectory(name, i),
                            new ArrayList<>(),
                            null,
                            0,
                            this.eventLogConfig.getMaxSegmentSizeBytes()
                    )
            );

            partitions.add(partition);
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

    private EventLogConfig setupEventLogConfig() {
        String rootDir = "data/topics";
        int maxSegmentSizeBytes = 10486576;
        return new EventLogConfig(Path.of(rootDir), maxSegmentSizeBytes);
    }
}
