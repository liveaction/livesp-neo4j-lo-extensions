package com.livingobjects.neo4j.model.exception;

public abstract class ImportException extends RuntimeException {

    ImportException(String message) {
        super(message);
    }

}
