package com.livingobjects.neo4j.loader;

import java.util.Optional;

public enum Status {

    CREATE,DELETE,UPDATE;

    public static String STATUS_HEADER ="status";

    public static Optional<Status> fromString(String val) {
        try {
            return Optional.of(Status.valueOf(val));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
