package com.axiomstream.core.service;

import com.axiomstream.api.dto.EventGetRequest;
import com.axiomstream.api.dto.EventGetResponse;

import java.io.IOException;

public interface IEventService {
    EventGetResponse getEvents(EventGetRequest request) throws IOException;
}
