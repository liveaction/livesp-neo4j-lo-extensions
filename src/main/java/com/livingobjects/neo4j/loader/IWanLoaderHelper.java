package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Optional;
import java.util.Set;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

final class IWanLoaderHelper {

    private static final String CLASS_SCOPE = "class:scope";

    static Optional<Scope> findScopeValue(IwanMappingStrategy strategy, Set<String> scopeTypes, String[] line) {
        Optional<Scope> oScope = strategy.getAllElementsType().stream()
                .filter(scopeTypes::contains)
                .map(kt -> Maps.immutableEntry(line[strategy.getColumnIndex(kt, ID)], line[strategy.getColumnIndex(kt, TAG)]))
                .filter(e -> !e.getValue().isEmpty())
                .map(idx -> new Scope(idx.getKey(), idx.getValue()))
                .findAny();

        if (oScope.isPresent()) return oScope;

        if (strategy.hasKeyType(CLASS_SCOPE)) {
            String tag = line[strategy.getColumnIndex(CLASS_SCOPE, TAG)];
            if (tag != null && !tag.isEmpty()) {
                String id = line[strategy.getColumnIndex(CLASS_SCOPE, ID)];
                return Optional.of(new Scope(id, tag));
            }
        }

        return Optional.empty();
    }

    static ImmutableSet<String> getCrossAttributes(Node node) {
        ImmutableSet.Builder<String> crossBldr = ImmutableSet.builder();
        node.getRelationships(Direction.OUTGOING, RelationshipTypes.CROSS_ATTRIBUTE).forEach(r -> {
            Node endNode = r.getEndNode();
            if (endNode.hasLabel(Labels.ATTRIBUTE)) {
                Object typeProperty = endNode.getProperty(IwanModelConstants._TYPE);
                Object nameProperty = endNode.getProperty(IwanModelConstants.NAME);
                if (typeProperty != null && nameProperty != null) {
                    String endKeytype = typeProperty.toString() + IwanModelConstants.KEYTYPE_SEPARATOR + nameProperty.toString();
                    crossBldr.add(endKeytype);
                }
            }
        });

        return crossBldr.build();
    }

    static Scope consolidateScope(Scope scope, boolean isOverridable, boolean isGlobal) {
        if (isOverridable) {
            if (scope != null)
                return scope;
            else
                return GLOBAL_SCOPE;
        } else if (isGlobal) {
            return GLOBAL_SCOPE;
        } else {
            return scope;
        }
    }

    static Optional<Node> getParent(Node attributeNode) {
        for (Relationship relationship : attributeNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PARENT)) {
            Object cardinality = relationship.getProperty(IwanModelConstants.CARDINALITY, IwanModelConstants.CARDINALITY_UNIQUE_PARENT);
            if (IwanModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
                return Optional.of(relationship.getEndNode());
            }
        }
        return Optional.empty();
    }
}
