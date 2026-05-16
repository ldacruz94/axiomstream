package com.axiomstream.core.service;

import com.axiomstream.api.dto.OffsetCommitRequest;
import com.axiomstream.api.dto.OffsetCommitResponse;
import com.axiomstream.api.dto.OffsetGetRequest;
import com.axiomstream.api.dto.OffsetGetResponse;

public interface IConsumerOffsetService {
    OffsetGetResponse getOffset(OffsetGetRequest request);
    OffsetCommitResponse commitOffset(OffsetCommitRequest request);
}
