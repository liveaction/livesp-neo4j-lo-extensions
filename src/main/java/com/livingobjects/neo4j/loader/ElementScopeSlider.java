package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.Set;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;

/**
 * See <a href="http://redmine.livingobjects.com/issues/12069">Redmine #12069</a>
 */
final class ElementScopeSlider {

    private final TemplatedPlanetFactory templatedPlanetFactory;
    private final Log log;

    ElementScopeSlider(TemplatedPlanetFactory templatedPlanetFactory, Log log) {
        this.templatedPlanetFactory = templatedPlanetFactory;
        this.log = log;
    }

    Node slide(Node element, Scope toScope, Transaction tx) {

        ImmutableSet<String> badScopes = changeElementPlanet(element, toScope, tx);
        removeImproperParents(element, badScopes);
        slideAllChildren(element, toScope, tx);

        return element;
    }

    private ImmutableSet<String> changeElementPlanet(Node element, Scope toScope, Transaction tx) {
        ImmutableSet.Builder<String> oldScopesBldr = ImmutableSet.builder();
        for (Relationship plRelation : element.getRelationships(Direction.OUTGOING, RelationshipTypes.ATTRIBUTE)) {
            Node planet = plRelation.getEndNode();
            Object oldScope = planet.getProperty(SCOPE, null);
            if (planet.hasLabel(Labels.PLANET) && !toScope.equals(oldScope)) {
                if (oldScope != null) oldScopesBldr.add(oldScope.toString());
                plRelation.delete();
            }
        }

        UniqueEntity<Node> planet = templatedPlanetFactory.localizePlanetForElement(toScope, element, tx);
        if (log.isDebugEnabled()) {
            String tag = element.getProperty(TAG).toString();
            String planetName = planet.entity.getProperty(NAME).toString();
            log.debug("Create link between (%s)-[:Attribute]->(%s) !", tag, planetName);
        }
        element.createRelationshipTo(planet.entity, RelationshipTypes.ATTRIBUTE);
        return oldScopesBldr.build();
    }

    private void removeImproperParents(Node element, Set<String> badScopes) {
        element.getRelationships(Direction.OUTGOING, RelationshipTypes.CONNECT, RelationshipTypes.CROSS_ATTRIBUTE).forEach(pRelation -> {
            for (Relationship plRelation : pRelation.getEndNode().getRelationships(Direction.OUTGOING, RelationshipTypes.ATTRIBUTE)) {
                Node planet = plRelation.getEndNode();
                if (planet.hasLabel(Labels.PLANET)) {
                    Object plName = planet.getProperty(SCOPE, null);
                    if (plName == null || badScopes.contains(plName.toString())) {
                        pRelation.delete();
                        break;
                    }
                }
            }
        });
    }

    private void slideAllChildren(Node entity, Scope toScope, Transaction tx) {
        entity.getRelationships(Direction.INCOMING, RelationshipTypes.CONNECT).forEach(childRelation -> {
            Node childNode = childRelation.getStartNode();
            if (!childNode.hasLabel(Labels.ELEMENT)) return;
            if (log.isDebugEnabled()) {
                String tag = childNode.getProperty(TAG).toString();
                log.debug("child slide %s !", tag);
            }
            slide(childNode, toScope, tx);
        });
    }
}
