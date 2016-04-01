package com.livingobjects.neo4j.model.status;

import com.livingobjects.neo4j.model.Neo4jError;
import org.codehaus.jackson.annotate.JsonProperty;

public class Failure extends Status {

    @JsonProperty("error")
    public final Neo4jError error;

    public Failure(Neo4jError error) {
        super(Type.FAILURE);
        this.error = error;
    }

    @Override
    public <T> T visit(Status.Visitor<T> visitor) {
        return visitor.failure(error);
    }

}
