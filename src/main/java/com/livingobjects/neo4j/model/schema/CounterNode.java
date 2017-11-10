package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;

public final class CounterNode {

    public final String type;
    public final String unit;
    public final String defaultValue;
    public final String defaultAggregation;
    public final String valueType;
    public final String name;
    public final String context;
    public final String _updatedAt;
    public final String _createdAt;

    public CounterNode(@JsonProperty("type") String type,
                       @JsonProperty("unit") String unit,
                       @JsonProperty("defaultValue") String defaultValue,
                       @JsonProperty("defaultAggregation") String defaultAggregation,
                       @JsonProperty("valueType") String valueType,
                       @JsonProperty("name") String name,
                       @JsonProperty("context") String context,
                       @JsonProperty("_updatedAt") @Nullable String _updatedAt,
                       @JsonProperty("_createdAt") @Nullable String _createdAt) {
        this.unit = unit;
        this.defaultValue = defaultValue;
        this.defaultAggregation = defaultAggregation;
        this.valueType = valueType;
        this.type = type;
        this.name = name;
        this.context = context;
        this._updatedAt = _updatedAt;
        this._createdAt = _createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CounterNode that = (CounterNode) o;

        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (unit != null ? !unit.equals(that.unit) : that.unit != null) return false;
        if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null) return false;
        if (defaultAggregation != null ? !defaultAggregation.equals(that.defaultAggregation) : that.defaultAggregation != null)
            return false;
        if (valueType != null ? !valueType.equals(that.valueType) : that.valueType != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (context != null ? !context.equals(that.context) : that.context != null) return false;
        if (_updatedAt != null ? !_updatedAt.equals(that._updatedAt) : that._updatedAt != null) return false;
        return _createdAt != null ? _createdAt.equals(that._createdAt) : that._createdAt == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        result = 31 * result + (defaultAggregation != null ? defaultAggregation.hashCode() : 0);
        result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (context != null ? context.hashCode() : 0);
        result = 31 * result + (_updatedAt != null ? _updatedAt.hashCode() : 0);
        result = 31 * result + (_createdAt != null ? _createdAt.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("unit", unit)
                .add("defaultValue", defaultValue)
                .add("defaultAggregation", defaultAggregation)
                .add("valueType", valueType)
                .add("name", name)
                .add("context", context)
                .add("_updatedAt", _updatedAt)
                .add("_createdAt", _createdAt)
                .toString();
    }
}

