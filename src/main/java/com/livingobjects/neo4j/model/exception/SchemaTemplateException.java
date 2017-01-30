package com.livingobjects.neo4j.model.exception;

public final class SchemaTemplateException extends RuntimeException {

    public SchemaTemplateException(String s) {
        super(s);
    }

    public SchemaTemplateException(String s, Throwable cause) {
        super(s, cause);
    }

}
