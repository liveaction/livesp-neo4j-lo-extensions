package com.livingobjects.neo4j.model.export;

import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

public final class Lineage {

    public final Map<String, Node> nodesByType;
    public final GraphDatabaseService graphDb;
    // Filled at the end of the process, with the properties to export only
    public final Map<String, Map<String, Object>> propertiesToExportByType;
    // Contains all properties used during the process, with lazy loading
    private final Map<String, Map<String, Object>> propertiesByType;

    private String repr;

    public Lineage(GraphDatabaseService graphDb) {
        this.nodesByType = new HashMap<>();
        this.graphDb = graphDb;
        this.propertiesToExportByType = new HashMap<>();
        this.propertiesByType = new HashMap<>();
    }

    @Override
    public String toString() {
        try {
            repr = nodesByType.values().stream()
                    .map(node -> node.getProperty(TAG).toString())
                    .collect(Collectors.joining(" - "));
            return repr;
        } catch (Exception e) {
            if (repr == null) {
                throw e;
            }
            return repr;
        }
    }


    public Object getProperty(String type, String property) {
        Optional<Object> optValue = Optional.ofNullable(propertiesByType.get(type)).map(values -> values.get(property));
        if (optValue.isPresent()) {
            return optValue.get();
        }
        Node node = nodesByType.get(type);
        Object value;
        if (node != null) {
            if (GraphModelConstants.SCOPE.equals(property)) {
                value = getElementScopeFromPlanet(node);
            } else {
                value = node.getProperty(property, null);
            }
            propertiesByType.computeIfAbsent(type, k -> new HashMap<>()).put(property, value);
            return value;
        } else {
            return null;
        }
    }

    public Set<String> getAllPropertiesForType(String keyAttribute) {
        Node node = nodesByType.get(keyAttribute);
        if (node == null) {
            return Set.of();
        }
        Map<String, Object> allProperties = node.getAllProperties();
        propertiesByType.computeIfAbsent(keyAttribute, k -> Maps.newHashMap()).putAll(allProperties);
        return allProperties.keySet();
    }

    private String getElementScopeFromPlanet(Node node) {
        Relationship planetRelationship = node.getSingleRelationship(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING);
        if (planetRelationship == null) {
            String tag = node.getProperty(TAG, "").toString();
            throw new IllegalArgumentException(String.format("%s %s=%s is not linked to a planet", Labels.NETWORK_ELEMENT, TAG, tag));
        }
        String scopeTag = planetRelationship.getEndNode().getProperty(SCOPE).toString();
        switch (scopeTag) {
            case SCOPE_GLOBAL_TAG:
                return GLOBAL_SCOPE.id;
            case SCOPE_SP_TAG:
                return SP_SCOPE.id;
            default:
                try (Transaction tx = graphDb.beginTx()) {
                    Node scopeNode = tx.findNode(Labels.NETWORK_ELEMENT, TAG, scopeTag);
                    if (scopeNode != null) {
                        String[] split = scopeNode.getProperty(_TYPE, ":").toString().split(":");
                        return split[1];
                    } else {
                        throw new IllegalArgumentException(String.format("Scope %s=%s cannot be found", TAG, scopeTag));
                    }
                }
        }
    }

}
