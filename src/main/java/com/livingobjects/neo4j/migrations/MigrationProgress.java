package com.livingobjects.neo4j.migrations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public final class MigrationProgress {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationProgress.class);
    private boolean terminated = false;
    private int status = HttpStatus.PARTIAL_CONTENT_206;

    private Queue<String> logs = Queues.newConcurrentLinkedQueue();

    public void log(String message) {
        logs.add(message);
        LOGGER.info(message);
    }

    public ImmutableList<String> getAllUnreadMessages() {
        List<String> collect = logs.stream().map(m -> logs.poll()).collect(Collectors.toList());
        return ImmutableList.copyOf(collect);
    }

    public void success() {
        terminated = true;
        status = HttpStatus.OK_200;
    }

    public void failed() {
        terminated = true;
        status = HttpStatus.INTERNAL_SERVER_ERROR_500;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public int getStatus() {
        return status;
    }
}
