package com.livingobjects.neo4j.loader;

import java.util.Optional;

public enum Action {

    DELETE_CASCADE_ALL, DELETE_CASCADE, DELETE_NO_CASCADE;

    public static String STATUS_HEADER = "action";

    public static Optional<Action> fromString(String val) {
        try {
            return Optional.of(Action.valueOf(val.toUpperCase()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
