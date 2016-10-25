package com.livingobjects.neo4j.iwan.model.schema.factories;

import com.livingobjects.neo4j.iwan.model.CacheIndexedElementFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.Iterator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_NETWORK_ELEMENT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_SCOPE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.TAG;

public final class ScopeNetworkElementFactory extends CacheIndexedElementFactory<String> {

    public ScopeNetworkElementFactory(GraphDatabaseService graphDB) {
        super(graphDB);
    }

    @Override
    protected Iterator<Node> initialLoad() {
        return graphDB.findNodes(LABEL_SCOPE);
    }

    @Override
    protected String readNode(Node node) {
        return getNullableStringProperty(node, TAG);
    }

    @Override
    protected Node createNode(String tag) {
        return createNodeFromStringKey(LABEL_NETWORK_ELEMENT, TAG, tag);
    }
}

