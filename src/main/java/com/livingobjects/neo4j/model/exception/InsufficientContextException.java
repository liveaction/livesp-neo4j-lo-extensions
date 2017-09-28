package com.livingobjects.neo4j.model.exception;

public class InsufficientContextException extends IllegalStateException {
    private static final long serialVersionUID = -781673142392486522L;

    public InsufficientContextException(String s) {
        super(s);
    }
}
