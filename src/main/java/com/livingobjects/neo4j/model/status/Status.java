package com.livingobjects.neo4j.model.status;

import com.livingobjects.neo4j.model.Neo4jError;
import com.livingobjects.neo4j.model.Neo4jResult;
import org.codehaus.jackson.annotate.JsonProperty;

public abstract class Status {

    public interface Visitor<T> {
        T inProgress();

        T success(Neo4jResult result);

        T failure(Neo4jError error);
    }

    public enum Type {IN_PROGRESS, SUCCESS, FAILURE}

    @JsonProperty("status")
    public final Type status;

    Status(Type status) {
        this.status = status;
    }

    public abstract <T> T visit(Visitor<T> visitor);

}
