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
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

public final class Lineage {

    public final Map<String, Node> nodesByType = Maps.newHashMap();

    public final GraphDatabaseService graphDb;

    public Lineage(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @Override
    public String toString() {
        return nodesByType.entrySet().stream().map(e -> e.getValue().getProperty(TAG).toString()).collect(Collectors.joining(" - "));
    }

    public Object getProperty(String keyAttribute, String property) {
        return getProperty(nodesByType.get(keyAttribute), property);
    }

    public Object getProperty(Node node, String property) {
        if (node != null) {
            if (GraphModelConstants.SCOPE.equals(property)) {
                return getElementScopeFromPlanet(node);
            } else {
                return node.getProperty(property, null);
            }
        } else {
            return null;
        }
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
