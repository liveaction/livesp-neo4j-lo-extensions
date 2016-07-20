package com.livingobjects.neo4j.iwan.model.exception;

public abstract class ImportException extends RuntimeException {

    ImportException(String message) {
        super(message);
    }

}
