package com.livingobjects.neo4j.helper;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.livingobjects.neo4j.model.PropertyType;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class PropertyConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyConverter.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final TypeReference STRING_LIST_TYPE = new TypeReference<List<String>>() {
    };
    private static final TypeReference BOOLEAN_LIST_TYPE = new TypeReference<List<Boolean>>() {
    };
    private static final TypeReference DOUBLE_LIST_TYPE = new TypeReference<List<Number>>() {
    };

    public static Object convert(String value, PropertyType propertyType, boolean isArray) {
        try {
            switch (propertyType) {
                case BOOLEAN:
                    return readBooleanField(isArray, value);
                case NUMBER:
                    return readNumberField(isArray, value);
                default:
                    return readStringField(isArray, value);
            }
        } catch (Exception ignored) {
            LOGGER.debug("Unable to parse value " + value + " as " + propertyType + (isArray ? "[]" : ""));
            return value;
        }
    }

    private static Object readStringField(boolean isArray, String field) throws IOException {
        if (field != null && !field.trim().isEmpty()) {
            if (isArray) {
                return Iterables.toArray(JSON_MAPPER.readValue(field, STRING_LIST_TYPE), String.class);
            } else {
                return field;
            }
        } else {
            return null;
        }
    }

    private static Object readBooleanField(boolean isArray, String field) throws IOException {
        if (field != null && !field.trim().isEmpty()) {
            if (isArray) {
                return Booleans.toArray(JSON_MAPPER.readValue(field, BOOLEAN_LIST_TYPE));
            } else {
                return Boolean.parseBoolean(field);
            }
        } else {
            return null;
        }
    }

    private static Object readNumberField(boolean isArray, String field) throws IOException {
        if (field != null && !field.trim().isEmpty()) {
            if (isArray) {
                Collection<? extends Number> numbers = JSON_MAPPER.readValue(field, DOUBLE_LIST_TYPE);
                Iterator<? extends Number> iterator = numbers.iterator();
                if (iterator.hasNext()) {
                    Number next = iterator.next();
                    if (next instanceof Short) {
                        return Shorts.toArray(numbers);
                    } else if (next instanceof Integer) {
                        return Ints.toArray(numbers);
                    } else if (next instanceof Long) {
                        return Longs.toArray(numbers);
                    } else if (next instanceof Float) {
                        return Floats.toArray(numbers);
                    } else {
                        return Doubles.toArray(numbers);
                    }
                } else {
                    return new int[0];
                }
            } else {
                try {
                    return JSON_MAPPER.readValue(field, Number.class);
                } catch (JsonMappingException e) {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

}
