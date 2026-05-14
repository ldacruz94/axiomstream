package com.axiomstream.core.service;

import com.axiomstream.api.dto.ProduceEventResponse;

import java.io.IOException;

public interface IProducerService {

    ProduceEventResponse produce(String topicName, String key, String payload) throws IOException;

}
