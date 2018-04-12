package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonIgnoreProperties(value = "label")
public final class CounterNode {

    public final String unit;
    public final String defaultValue;
    public final String defaultAggregation;
    public final String valueType;
    public final String name;

    @Nullable
    public final String description;

    public CounterNode(@JsonProperty("unit") String unit,
                       @JsonProperty("defaultValue") String defaultValue,
                       @JsonProperty("defaultAggregation") String defaultAggregation,
                       @JsonProperty("valueType") String valueType,
                       @JsonProperty("name") String name,
                       @JsonProperty("description") @Nullable String description) {
        this.unit = unit;
        this.defaultValue = defaultValue;
        this.defaultAggregation = defaultAggregation;
        this.valueType = valueType;
        this.name = name;
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CounterNode that = (CounterNode) o;
        return Objects.equals(unit, that.unit) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(defaultAggregation, that.defaultAggregation) &&
                Objects.equals(valueType, that.valueType) &&
                Objects.equals(name, that.name) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {

        return Objects.hash(unit, defaultValue, defaultAggregation, valueType, name, description);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("unit", unit)
                .add("defaultValue", defaultValue)
                .add("defaultAggregation", defaultAggregation)
                .add("valueType", valueType)
                .add("name", name)
                .add("description", description)
                .toString();
    }
}

