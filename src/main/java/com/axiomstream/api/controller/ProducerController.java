package com.axiomstream.api.controller;

import com.axiomstream.api.dto.ErrorResponse;
import com.axiomstream.api.dto.ProduceEventRequest;
import com.axiomstream.api.dto.ProduceEventResponse;
import com.axiomstream.core.exceptions.TopicDoesNotExist;
import com.axiomstream.core.service.ProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/topics")
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/{topicName}/events")
    public ResponseEntity<?> create(@PathVariable String topicName, @RequestBody ProduceEventRequest req) {
        try {
            ProduceEventResponse res = producerService.produce(topicName, req.getKey(), req.getPayload());

            return ResponseEntity
                    .status(HttpStatus.CREATED)
                    .body(res);

        } catch (IllegalArgumentException | IOException | TopicDoesNotExist e) {
            ErrorResponse errorResponse = new ErrorResponse(
                    "INVALID_REQUEST",
                    e.getMessage()
            );

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }
}