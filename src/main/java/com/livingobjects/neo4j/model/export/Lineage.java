package com.livingobjects.neo4j.model.export;

import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.stream.Collectors;

public final class Lineage {
    public final Map<String, Node> nodesByType;

    public Lineage() {
        this.nodesByType = Maps.newHashMap();
    }

    @Override
    public String toString() {
        return nodesByType.entrySet().stream().map(e -> e.getValue().getProperty(IwanModelConstants.TAG).toString()).collect(Collectors.joining(" - "));
    }
}
