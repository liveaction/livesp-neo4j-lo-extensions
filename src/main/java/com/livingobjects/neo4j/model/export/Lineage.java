package com.livingobjects.neo4j.model.export;

import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

public final class Lineage {

    private final GraphDatabaseService graphDb;
    private final Lineages lineages;

    private final Map<String, Map<String, Object>> properties = Maps.newConcurrentMap();

    public final Map<String, Node> nodesByType = Maps.newHashMap();

    public Lineage(GraphDatabaseService graphDb, Lineages lineages) {
        this.graphDb = graphDb;
        this.lineages = lineages;
    }

    @Override
    public String toString() {
        return nodesByType.entrySet().stream().map(e -> e.getValue().getProperty(GraphModelConstants.TAG).toString()).collect(Collectors.joining(" - "));
    }

    public Map<String, Object> getProperties(String keyAttribute) {
        return properties.computeIfAbsent(keyAttribute, k -> buildProperties(keyAttribute));
    }

    private Map<String, Object> buildProperties(String keyAttribute) {
        Node node = nodesByType.get(keyAttribute);
        Map<String, Object> values = Maps.newLinkedHashMap();
        SortedMap<String, String> properties = lineages.propertiesTypeByType.get(keyAttribute);
        if (node == null) {
            if (properties != null) {
                properties.keySet().forEach(property -> values.put(property, null));
            }
        } else if (properties != null) {
            for (String property : properties.keySet()) {
                Object propertyValue;
                if (property.equals(SCOPE)) {
                    propertyValue = getElementScopeFromPlanet(node);
                } else {
                    propertyValue = node.getProperty(property, null);
                }
                values.put(property, propertyValue);
            }
        }
        return values;
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
                Node scopeNode = graphDb.findNode(Labels.NETWORK_ELEMENT, TAG, scopeTag);
                if (scopeNode != null) {
                    String[] split = scopeNode.getProperty(_TYPE, ":").toString().split(":");
                    return split[1];
                } else {
                    throw new IllegalArgumentException(String.format("Scope %s=%s cannot be found", TAG, scopeTag));
                }
        }
    }

}
