package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public final class Counter {

    public String unit;

    public String name;

    public String context;

    public String defaultAggregation;

    public String valueType;

    public Double defaultValue;

    public Counter() {
    }

    public Counter(
            String unit,
            String name,
            String context,
            String defaultAggregation,
            String valueType,
            Double defaultValue) {
        this.unit = unit;
        this.name = name;
        this.context = context;
        this.defaultAggregation = defaultAggregation;
        this.valueType = valueType;
        this.defaultValue = defaultValue;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getDefaultAggregation() {
        return defaultAggregation;
    }

    public void setDefaultAggregation(String defaultAggregation) {
        this.defaultAggregation = defaultAggregation;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public Double getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Double defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Counter counter = (Counter) o;
        return Objects.equals(unit, counter.unit) &&
                Objects.equals(name, counter.name) &&
                Objects.equals(context, counter.context) &&
                Objects.equals(defaultAggregation, counter.defaultAggregation) &&
                Objects.equals(valueType, counter.valueType) &&
                Objects.equals(defaultValue, counter.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit, name, context, defaultAggregation, valueType, defaultValue);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("unit", unit)
                .add("name", name)
                .add("context", context)
                .add("defaultAggregation", defaultAggregation)
                .add("valueType", valueType)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
