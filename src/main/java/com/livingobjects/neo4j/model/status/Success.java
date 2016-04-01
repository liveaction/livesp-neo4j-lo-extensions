package com.livingobjects.neo4j.model.status;

import com.livingobjects.neo4j.model.Neo4jResult;
import org.codehaus.jackson.annotate.JsonProperty;

public class Success extends Status {

    @JsonProperty("result")
    public final Neo4jResult result;

    public Success(Neo4jResult result) {
        super(Type.SUCCESS);
        this.result = result;
    }

    @Override
    public <T> T visit(Status.Visitor<T> visitor) {
        return visitor.success(result);
    }
}
