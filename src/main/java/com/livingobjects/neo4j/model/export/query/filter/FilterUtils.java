package com.livingobjects.neo4j.model.export.query.filter;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;

public class FilterUtils {

    public static boolean testFilter(Object value, ValueFilter valueFilter) {
        boolean b = testFilterInternal(value, valueFilter);
        if (valueFilter.not) {
            return !b;
        } else {
            return b;
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean testFilterInternal(Object value, ValueFilter valueFilter) {
        switch (valueFilter.operator) {
            case eq:
                return valueFilter.value.equals(value);
            case lt:
                return valueFilter.value.compareTo(value) < 0;
            case lte:
                return valueFilter.value.compareTo(value) <= 0;
            case gt:
                return valueFilter.value.compareTo(value) > 0;
            case gte:
                return valueFilter.value.compareTo(value) >= 0;
            case regex:
                return asNonNullString(value).matches(valueFilter.value.toString());
            case is_null:
                return value == null;
            case contains:
                if (value.getClass().isArray()) {
                    Object[] array = (Object[]) value;
                    return ImmutableSet.copyOf(array).contains(valueFilter.value);
                } else if (value instanceof Iterable) {
                    Iterable iterable = (Iterable) value;
                    return ImmutableSet.copyOf(iterable).contains(valueFilter.value);
                } else {
                    return false;
                }
            default:
                throw new IllegalArgumentException("Unsupported operator '" + valueFilter.operator + "'. Must be one of : " + Arrays.toString(ValueFilter.Operator.values()));
        }
    }

    private static String asNonNullString(Object value) {
        if (value != null) {
            return value.toString();
        } else {
            return "";
        }
    }

}
