package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Optional;

final class CsvLoaderHelper {

    private static final String CLASS_SCOPE = "class:scope";

    static ImmutableSet<String> getCrossAttributes(Node node) {
        ImmutableSet.Builder<String> crossBldr = ImmutableSet.builder();
        node.getRelationships(Direction.OUTGOING, RelationshipTypes.CROSS_ATTRIBUTE).forEach(r -> {
            Node endNode = r.getEndNode();
            if (endNode.hasLabel(Labels.ATTRIBUTE)) {
                Object typeProperty = endNode.getProperty(GraphModelConstants._TYPE);
                Object nameProperty = endNode.getProperty(GraphModelConstants.NAME);
                if (typeProperty != null && nameProperty != null) {
                    String endKeytype = typeProperty.toString() + GraphModelConstants.KEYTYPE_SEPARATOR + nameProperty.toString();
                    crossBldr.add(endKeytype);
                }
            }
        });

        return crossBldr.build();
    }

    static Optional<Node> getParent(Node attributeNode) {
        for (Relationship relationship : attributeNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PARENT)) {
            Object cardinality = relationship.getProperty(GraphModelConstants.CARDINALITY, GraphModelConstants.CARDINALITY_UNIQUE_PARENT);
            if (GraphModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
                return Optional.of(relationship.getEndNode());
            }
        }
        return Optional.empty();
    }
}
