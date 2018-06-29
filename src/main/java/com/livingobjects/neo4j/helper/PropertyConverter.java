package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.livingobjects.neo4j.model.PropertyType;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.helpers.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class PropertyConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyConverter.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final TypeReference STRING_LIST_TYPE = new TypeReference<String[]>() {
    };
    private static final TypeReference BOOLEAN_LIST_TYPE = new TypeReference<Boolean[]>() {
    };
    private static final TypeReference NUMBER_LIST_TYPE = new TypeReference<Number[]>() {
    };

    public static Object checkPropertyValue(Object value) {
        if (value instanceof String) {
            return value;
        } else if (value instanceof Number) {
            return value;
        } else if (value instanceof Iterable) {
            return Iterables.toArray((Iterable) value);
        } else {
            return null;
        }
    }

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
                return JSON_MAPPER.readValue(field, STRING_LIST_TYPE);
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
                return JSON_MAPPER.readValue(field, BOOLEAN_LIST_TYPE);
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
                boolean jacksonCannotParse = false;
                String[] split = field.replaceAll("[\\[|\\]]", "").split(",");

                for (int i = 0; i < split.length; i++) {
                    if (split[i].startsWith(".")) {
                        jacksonCannotParse = true;
                        break;
                    }
                }

                Collection<? extends Number> numbers = jacksonCannotParse ?
                        convertAsDouble(split) :
                        ImmutableList.copyOf((Number[]) JSON_MAPPER.readValue(field, NUMBER_LIST_TYPE));

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
                    if (field.startsWith(".")) {
                        return Double.valueOf(field);
                    }

                    return JSON_MAPPER.readValue(field, Number.class);
                } catch (JsonMappingException e) {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    private static Collection<? extends Number> convertAsDouble(String[] doubleFieldArray) {
        List<Number> doubles = new LinkedList<>();

        for (int i = 0; i < doubleFieldArray.length; i++) {
            doubles.add(Double.valueOf(doubleFieldArray[i]));
        }

        return doubles;
    }

}
