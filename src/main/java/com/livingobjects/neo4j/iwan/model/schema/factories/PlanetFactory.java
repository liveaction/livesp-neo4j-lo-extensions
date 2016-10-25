package com.livingobjects.neo4j.iwan.model.schema.factories;

import com.livingobjects.neo4j.iwan.model.CacheIndexedElementFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.Iterator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_PLANET;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.NAME;

public class PlanetFactory extends CacheIndexedElementFactory<String> {

    public PlanetFactory(GraphDatabaseService graphDB) {
        super(graphDB);
    }

    @Override
    protected Iterator<Node> initialLoad() {
        return graphDB.findNodes(LABEL_PLANET);
    }

    @Override
    protected String readNode(Node node) {
        return getNullableStringProperty(node, NAME);
    }

    @Override
    protected Node createNode(String planetName) {
        return createNodeFromStringKey(LABEL_PLANET, NAME, planetName);
    }
}
