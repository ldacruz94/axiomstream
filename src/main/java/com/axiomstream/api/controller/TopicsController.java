package com.axiomstream.api.controller;

import com.axiomstream.api.dto.ErrorResponse;
import com.axiomstream.api.dto.TopicCreateRequest;
import com.axiomstream.api.dto.TopicGetResponse;
import com.axiomstream.core.model.Topic;
import com.axiomstream.core.service.TopicService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/topics")
public class TopicsController {

    private final TopicService topicService;

    public TopicsController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping("/{topicName}")
    public ResponseEntity<?> get(@PathVariable String topicName) {
        try {
            Topic topic = topicService.getTopic(topicName);

            TopicGetResponse response = new TopicGetResponse(
                    topic.getName(),
                    topic.getPartitionCount()
            );

            return ResponseEntity.ok(response);

        } catch (NoSuchElementException ex) {
            ErrorResponse error = new ErrorResponse(
                    "TOPIC_NOT_FOUND",
                    ex.getMessage()
            );

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody TopicCreateRequest req) {
        try {
            Topic topic = topicService.createTopic(
                    req.getName(),
                    req.getPartitions()
            );

            TopicGetResponse response = new TopicGetResponse(
                    topic.getName(),
                    topic.getPartitionCount()
            );

            return ResponseEntity
                    .created(URI.create("/topics/" + topic.getName()))
                    .body(response);

        } catch (IllegalArgumentException ex) {
            ErrorResponse error = new ErrorResponse(
                    "CREATE_FAILED",
                    ex.getMessage()
            );

            return ResponseEntity.badRequest().body(error);
        }
    }
}