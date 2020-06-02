package com.livingobjects.neo4j.loader;

import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static java.util.Objects.requireNonNull;

public final class TopologyLoaderUtils {

    private final UniqueElementFactory scopeElementFactory;

    public TopologyLoaderUtils(UniqueElementFactory scopeElementFactory) {
        this.scopeElementFactory = requireNonNull(scopeElementFactory);
    }

    public Scope getScope(Node node) {
        return getScopeFromElementPlanet(node).orElseThrow(() -> new IllegalArgumentException("An element should have a scope"));
    }

    public Optional<Scope> getScopeFromElementPlanet(Node node) {
        Iterator<Relationship> iterator = node.getRelationships(Direction.OUTGOING,RelationshipTypes.ATTRIBUTE).iterator();

        if (iterator.hasNext()) {
            Relationship attributeRelationship = iterator.next();
            Node planetNode = attributeRelationship.getEndNode();
            String scopeTag = planetNode.getProperty(SCOPE, SCOPE_GLOBAL_TAG).toString();
            return Optional.of(readScopeFromTag(scopeTag));
        } else {
            return Optional.empty();
        }
    }

    private Scope readScopeFromTag(String scopeTag) {
        if (scopeTag.equals(GLOBAL_SCOPE.tag)) {
            return GLOBAL_SCOPE;
        } else if (scopeTag.equals(SP_SCOPE.tag)) {
            return SP_SCOPE;
        } else {
            Node scope = scopeElementFactory.getWithOutcome(TAG, scopeTag);
            if (scope == null) {
                throw new IllegalStateException(String.format("The scope %s cannot be found in database", scopeTag));
            }
            String id = scope.getProperty(ID).toString();
            return new Scope(id, scopeTag);
        }
    }
}
