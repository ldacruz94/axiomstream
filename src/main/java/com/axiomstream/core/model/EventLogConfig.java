package com.axiomstream.core.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.file.Path;

@Getter
@AllArgsConstructor
public class EventLogConfig {

    private Path rootDataDirectory;
    private long maxSegmentSizeBytes;

    public Path resolvePartitionDirectory(String topicName, int partitionId) {
        return rootDataDirectory.resolve(topicName + partitionId);
    }
}
