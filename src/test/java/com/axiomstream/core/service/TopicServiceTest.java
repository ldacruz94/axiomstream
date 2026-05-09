package com.axiomstream.core.service;

import com.axiomstream.core.exceptions.PartitionCountIsZeroException;
import com.axiomstream.core.exceptions.TopicAlreadyExistsException;
import com.axiomstream.core.model.Topic;
import com.axiomstream.core.registry.TopicRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopicServiceTest {

    private TopicRegistry topicRegistry;
    private TopicService topicService;

    @BeforeEach
    void setUp() {
        topicRegistry = new TopicRegistry();
        topicService = new TopicService(topicRegistry);
    }

    @Test
    void createTopic_shouldCreateTopicWithPartitions() {
        topicService.createTopic("orders", 3);

        Topic topic = topicService.getTopic("orders");

        assertNotNull(topic);
        assertEquals("orders", topic.getName());
        assertEquals(3, topic.getPartitionCount());
        assertEquals(3, topic.getPartitions().size());

        assertEquals(0, topic.getPartitions().get(0).getId());
        assertEquals(1, topic.getPartitions().get(1).getId());
        assertEquals(2, topic.getPartitions().get(2).getId());
    }

    @Test
    void createTopic_shouldStoreTopicInRegistry() {
        topicService.createTopic("payments", 2);

        Topic topic = topicRegistry.getTopic("payments");

        assertNotNull(topic);
        assertEquals("payments", topic.getName());
    }

    @Test
    void getTopic_shouldReturnExistingTopic() {
        topicService.createTopic("shipments", 1);

        Topic topic = topicService.getTopic("shipments");

        assertNotNull(topic);
        assertEquals("shipments", topic.getName());
    }

    @Test
    void listTopics_shouldReturnAllCreatedTopics() {
        topicService.createTopic("orders", 3);
        topicService.createTopic("payments", 2);

        List<Topic> topics = topicService.listTopics();

        assertEquals(2, topics.size());
    }

    @Test
    void createTopic_shouldThrowExceptionWhenPartitionCountIsZero() {
        assertThrows(
                PartitionCountIsZeroException.class,
                () -> topicService.createTopic("orders", 0)
        );
    }

    @Test
    void createTopic_shouldThrowExceptionWhenTopicAlreadyExists() {
        topicService.createTopic("orders", 3);

        assertThrows(
                TopicAlreadyExistsException.class,
                () -> topicService.createTopic("orders", 2)
        );
    }
}