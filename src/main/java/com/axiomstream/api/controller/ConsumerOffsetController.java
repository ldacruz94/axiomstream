package com.axiomstream.api.controller;


import com.axiomstream.api.dto.OffsetCommitRequest;
import com.axiomstream.api.dto.OffsetCommitResponse;
import com.axiomstream.api.dto.OffsetGetRequest;
import com.axiomstream.api.dto.OffsetGetResponse;
import com.axiomstream.core.service.ConsumerOffsetService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/consumer")
public class ConsumerOffsetController {

    private final ConsumerOffsetService consumerOffsetService;

    public ConsumerOffsetController(ConsumerOffsetService consumerOffsetService){
        this.consumerOffsetService = consumerOffsetService;
    }

    @PostMapping("/offsets/commit")
    public ResponseEntity<?> commit(@RequestBody OffsetCommitRequest req) {
        try {
            OffsetCommitResponse response = this.consumerOffsetService.commitOffset(req);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

    @GetMapping("/offsets")
    public ResponseEntity<?> get(
            @RequestParam String consumerId,
            @RequestParam String topicName,
            @RequestParam int partitionId
    ) {
        try {
            OffsetGetRequest req = new OffsetGetRequest(consumerId, topicName, partitionId);
            OffsetGetResponse response = this.consumerOffsetService.getOffset(req);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

}
