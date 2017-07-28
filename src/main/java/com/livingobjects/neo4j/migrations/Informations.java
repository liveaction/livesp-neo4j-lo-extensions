package com.livingobjects.neo4j.migrations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public final class Informations {
    private static final Logger LOGGER = LoggerFactory.getLogger(Informations.class);
    private boolean terminated = false;
    private int status = -1;

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
        status = 0;
    }

    public void failed() {
        terminated = true;
        status = -1;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public int getStatus() {
        return status;
    }
}
