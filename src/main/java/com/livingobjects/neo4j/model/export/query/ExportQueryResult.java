package com.livingobjects.neo4j.model.export.query;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents the result of a single ExportQuery
 */
public class ExportQueryResult {

    public final ImmutableMap<String, Map<String, Object>> result;

    public ExportQueryResult(ImmutableMap<String, Map<String, Object>> result) {
        this.result = result;
    }
}
