package com.axiomstream.api.controller;


import com.axiomstream.api.dto.ErrorResponse;
import com.axiomstream.api.dto.EventGetRequest;
import com.axiomstream.api.dto.EventGetResponse;
import com.axiomstream.core.service.EventService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/topics")
public class EventsController {

    private final EventService eventService;

    public EventsController(EventService eventService){
        this.eventService = eventService;
    }

    @GetMapping("/{topicName}/partitions/{partitionId}/events")
    public ResponseEntity<?> get(
            @PathVariable String topicName,
            @PathVariable int partitionId,
            @RequestParam long offset,
            @RequestParam int maxEvents
    ){

        try {
            EventGetRequest request = new EventGetRequest(topicName, partitionId, offset, maxEvents);
            EventGetResponse response = this.eventService.getEvents(request);
            return ResponseEntity.ok(response);
        } catch (IOException e) {
            ErrorResponse errorResponse = new ErrorResponse(
                    "ERROR",
                    e.getMessage()
            );

            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .body(errorResponse);
        }
    }
}
