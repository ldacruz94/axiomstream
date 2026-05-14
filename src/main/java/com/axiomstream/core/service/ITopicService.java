package com.axiomstream.core.service;

import com.axiomstream.core.model.Topic;
import java.util.List;

public interface ITopicService {

    Topic createTopic(String name, int partitionCount);
    Topic getTopic(String name);
    List<Topic> listTopics();

}
