package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(value = "label")
public final class CounterNode {

    public final String unit;
    public final String defaultValue;
    public final String defaultAggregation;
    public final String valueType;
    public final String name;

    public CounterNode(@JsonProperty("unit") String unit,
                       @JsonProperty("defaultValue") String defaultValue,
                       @JsonProperty("defaultAggregation") String defaultAggregation,
                       @JsonProperty("valueType") String valueType,
                       @JsonProperty("name") String name) {
        this.unit = unit;
        this.defaultValue = defaultValue;
        this.defaultAggregation = defaultAggregation;
        this.valueType = valueType;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CounterNode that = (CounterNode) o;

        if (unit != null ? !unit.equals(that.unit) : that.unit != null) return false;
        if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null) return false;
        if (defaultAggregation != null ? !defaultAggregation.equals(that.defaultAggregation) : that.defaultAggregation != null)
            return false;
        if (valueType != null ? !valueType.equals(that.valueType) : that.valueType != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = unit != null ? unit.hashCode() : 0;
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        result = 31 * result + (defaultAggregation != null ? defaultAggregation.hashCode() : 0);
        result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("unit", unit)
                .add("defaultValue", defaultValue)
                .add("defaultAggregation", defaultAggregation)
                .add("valueType", valueType)
                .add("name", name)
                .toString();
    }
}

