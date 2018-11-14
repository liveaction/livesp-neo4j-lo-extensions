package com.livingobjects.neo4j.model.schema.type.type;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CountCounterType.class, name = CounterType.COUNT)
})
public abstract class CounterType {

    public static final String COUNT = "count";

    public abstract <R> R visit(CounterType.Visitor<R> visitor);

    public interface Visitor<R> {

        R count(CountCounterType countCounterType);

    }

}
