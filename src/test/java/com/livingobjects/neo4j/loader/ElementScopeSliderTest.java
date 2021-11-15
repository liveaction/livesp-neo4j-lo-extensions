package com.livingobjects.neo4j.loader;

import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.rules.WithNeo4jImpermanentDatabase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static org.assertj.core.api.Assertions.assertThat;

public class ElementScopeSliderTest {

    @Rule
    public WithNeo4jImpermanentDatabase wNeo = new WithNeo4jImpermanentDatabase()
            .withDatapacks("neo4j-test-database");

    private ElementScopeSlider tested;

    @Before
    public void setUp() {
        GraphDatabaseService graphDb = wNeo.getGraphDatabaseService();
        try (Transaction ignore = graphDb.beginTx()) {
            TemplatedPlanetFactory templatedPlanetFactory = new TemplatedPlanetFactory(graphDb);
            tested = new ElementScopeSlider(templatedPlanetFactory);
        }
    }

    @Test
    public void slide() {
        GraphDatabaseService graphDb = wNeo.getGraphDatabaseService();
        try (Transaction tx = graphDb.beginTx()) {
            Node element = tx.findNode(Labels.NETWORK_ELEMENT, TAG, "class=neType,cpe=AA_RJ45,neType=cpe");

            Scope expectedScope = new Scope("boots", "class=cluster,client=boots,cluster=client");
            Node actual = tested.slide(element, expectedScope, tx);

            Node planetNode = actual.getSingleRelationship(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING).getEndNode();
            assertThat(planetNode.getProperty(SCOPE).toString()).isEqualTo(expectedScope.tag);
        }
    }

}