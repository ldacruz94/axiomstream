package com.axiomstream.core.store;

import com.axiomstream.core.model.ConsumerOffsetKey;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ConsumerOffsetStore {

    private final Map<ConsumerOffsetKey, Long> offsets = new ConcurrentHashMap<>();

    public long get(ConsumerOffsetKey key) {
        return offsets.get(key);
    }

    public void put(ConsumerOffsetKey key, long offset) {
        offsets.put(key, offset);
    }
}
