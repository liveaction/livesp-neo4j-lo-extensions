package com.livingobjects.neo4j.model.status;

import org.codehaus.jackson.annotate.JsonProperty;

public abstract class Status {

    public enum Type {IN_PROGRESS, SUCCESS, FAILURE}

    @JsonProperty("message")
    public final Type status;

    Status(Type status) {
        this.status = status;
    }
}
