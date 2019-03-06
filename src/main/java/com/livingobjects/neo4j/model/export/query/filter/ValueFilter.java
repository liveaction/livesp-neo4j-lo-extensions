package com.livingobjects.neo4j.model.export.query.filter;

import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

public final class ValueFilter {

    public enum Operator {
        lt, lte, gt, gte, eq, contains, regex, is_null
    }

    public final boolean not;
    public final Operator operator;
    public final Comparable value;

    private ValueFilter(@JsonProperty("not") boolean not,
                        @JsonProperty("operator") Operator operator,
                        @JsonProperty("value") Object value) {
        this.not = not;
        this.operator = operator;
        if (value != null) {
            if (value instanceof Comparable) {
                this.value = (Comparable) value;
            } else {
                throw new IllegalArgumentException("Value must be a comparable : " + value);
            }
        } else {
            this.value = null;
        }
    }

    /**
     * Test value against this filter.
     *
     * @param value a nullable value
     * @return true if the value pass the filter test, false otherwise.
     */
    public boolean test(@Nullable Object value) {
        return FilterUtils.testFilter(value, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueFilter that = (ValueFilter) o;
        return not == that.not &&
                operator == that.operator &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(not, operator, value);
    }

    @Override
    public String toString() {
        return String.format("[%s%s]%s", not ? "!" : "", operator, value);
    }

}
