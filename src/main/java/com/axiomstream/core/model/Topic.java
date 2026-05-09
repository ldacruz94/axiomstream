package com.axiomstream.core.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.Instant;
import java.util.List;

@Getter
@ToString
@AllArgsConstructor
public class Topic {

    @NonNull
    private final String name;
    private final int partitionCount;
    private List<Partition> partitions;
    private final Instant createdAt;

}
