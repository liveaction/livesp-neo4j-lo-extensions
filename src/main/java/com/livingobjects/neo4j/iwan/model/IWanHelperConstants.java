package com.livingobjects.neo4j.iwan.model;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.util.List;

public final class IWanHelperConstants {
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static final TypeReference STRING_LIST_TYPE = new TypeReference<List<String>>() {
    };
    public static final TypeReference BOOLEAN_LIST_TYPE = new TypeReference<List<Boolean>>() {
    };
    public static final TypeReference DOUBLE_LIST_TYPE = new TypeReference<List<Double>>() {
    };
}
