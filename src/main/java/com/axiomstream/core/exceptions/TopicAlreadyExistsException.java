package com.axiomstream.core.exceptions;

public class TopicAlreadyExistsException extends RuntimeException {
    public TopicAlreadyExistsException(String message) {
        super(message);
    }
}
