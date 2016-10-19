package com.livingobjects.neo4j.iwan.model.export;

import com.google.common.collect.Maps;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.TAG;

public final class Lineage {
    public final Map<String, Node> nodesByType;

    public Lineage() {
        this.nodesByType = Maps.newHashMap();
    }

    @Override
    public String toString() {
        return nodesByType.entrySet().stream().map(e -> e.getValue().getProperty(TAG).toString()).collect(Collectors.joining(" - "));
    }
}