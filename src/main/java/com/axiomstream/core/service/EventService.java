package com.axiomstream.core.service;

import com.axiomstream.api.dto.EventGetRequest;
import com.axiomstream.api.dto.EventGetResponse;
import com.axiomstream.core.model.EventRecord;
import com.axiomstream.core.model.Partition;
import com.axiomstream.core.model.Topic;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class EventService implements IEventService {

    private final TopicService topicService;

    public EventService(TopicService topicService) {
        this.topicService = topicService;
    }

    @Override
    public EventGetResponse getEvents(EventGetRequest request) throws IOException {
        Topic topic = topicService.getTopic(request.getTopicName());

        List<Partition> partitions = topic.getPartitions();
        Partition partition = partitions.get(request.getPartitionId());

        List<EventRecord> events = partition.readEvents(request.getOffset(), request.getMaxEvents());
        return new EventGetResponse(
                events.size(),
                partition.getEventLog().getNextOffset(),
                events
        );
    }
}
