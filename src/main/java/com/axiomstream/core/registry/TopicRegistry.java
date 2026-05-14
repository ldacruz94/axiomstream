package com.axiomstream.core.registry;

import com.axiomstream.core.model.Topic;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
public class TopicRegistry {

    private final HashMap<String, Topic> topicStore = new HashMap<>();

    public void createTopic(Topic topic) {
        topicStore.put(topic.getName(), topic);
    }

    public Topic getTopic(String name) {
        return topicStore.get(name);
    }

    public List<Topic> listTopics() {
        return new ArrayList<>(topicStore.values());
    }
}
