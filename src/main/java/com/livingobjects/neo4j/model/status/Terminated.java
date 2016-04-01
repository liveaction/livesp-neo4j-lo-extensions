package com.livingobjects.neo4j.model.status;

import com.livingobjects.neo4j.model.Neo4jError;
import com.livingobjects.neo4j.model.Neo4jResult;

public abstract class Terminated extends Status {

    public interface Visitor<T> {
        T success(Neo4jResult result);

        T failure(Neo4jError error);
    }

    public Terminated(Type status) {
        super(status);
    }

    public abstract <T> T visit(Terminated.Visitor<T> visitor);

}
