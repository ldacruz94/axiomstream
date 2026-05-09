package com.axiomstream.core.service;

import com.axiomstream.core.model.Topic;

import java.util.List;

public interface ITopicService {

    public void createTopic(String name, int partitionCount);
    public Topic getTopic(String name);
    public List<Topic> listTopics();

}
