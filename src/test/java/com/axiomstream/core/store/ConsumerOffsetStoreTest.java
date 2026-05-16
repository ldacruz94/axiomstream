package com.axiomstream.core.store;

import com.axiomstream.core.model.ConsumerOffsetKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumerOffsetStoreTest {

    private ConsumerOffsetStore consumerOffsetStore;

    @BeforeEach
    void setUp() {
        consumerOffsetStore = new ConsumerOffsetStore();
    }

    @Test
    void putShouldStoreOffsetForKey() {
        ConsumerOffsetKey key = new ConsumerOffsetKey("consumer-1", "topic-1", 0);

        consumerOffsetStore.put(key, 10L);

        assertEquals(10L, consumerOffsetStore.get(key));
    }

    @Test
    void putShouldOverwriteExistingOffsetForSameKey() {
        ConsumerOffsetKey key = new ConsumerOffsetKey("consumer-1", "topic-1", 0);

        consumerOffsetStore.put(key, 10L);
        consumerOffsetStore.put(key, 25L);

        assertEquals(25L, consumerOffsetStore.get(key));
    }

    @Test
    void getShouldReturnOffsetForEquivalentKey() {
        ConsumerOffsetKey originalKey = new ConsumerOffsetKey("consumer-1", "topic-1", 0);
        ConsumerOffsetKey equivalentKey = new ConsumerOffsetKey("consumer-1", "topic-1", 0);

        consumerOffsetStore.put(originalKey, 15L);

        assertEquals(15L, consumerOffsetStore.get(equivalentKey));
    }

    @Test
    void getShouldThrowNullPointerExceptionWhenKeyDoesNotExist() {
        ConsumerOffsetKey missingKey = new ConsumerOffsetKey("consumer-1", "topic-1", 0);

        assertThrows(NullPointerException.class, () -> consumerOffsetStore.get(missingKey));
    }
}