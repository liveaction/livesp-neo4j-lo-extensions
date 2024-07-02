package com.livingobjects.neo4j.loader;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.rules.WithNeo4jImpermanentDatabase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CsvTopologyLoaderTest {

    @Rule
    public WithNeo4jImpermanentDatabase wNeo = new WithNeo4jImpermanentDatabase()
            .withDatapacks("neo4j-test-database");
    private CsvTopologyLoader tested;

    @Before
    public void setUp() {
        tested = new CsvTopologyLoader(wNeo.getGraphDatabaseService(), new MetricRegistry());
    }

    @Test
    public void shouldSortElementsToDelete() {
        List<String> actual;
        try (Transaction tx = wNeo.getGraphDatabaseService().beginTx()) {
            actual = tested.sortKeyTypes(ImmutableSet.of("cluster:client", "cluster:site", "neType:cpe", "cluster:application/group",
                    "neType:viewpoint", "neType:wanLink", "cluster:area", "neType:application", "neType:cos"), tx);
        }
        assertThat(actual.indexOf("neType:viewpoint")).isLessThan(actual.indexOf("neType:cpe"));
        assertThat(actual.indexOf("neType:wanLink")).isLessThan(actual.indexOf("neType:cpe"));
        assertThat(actual.indexOf("cluster:site")).isLessThan(actual.indexOf("cluster:client"));
        assertThat(actual.indexOf("neType:cpe")).isLessThan(actual.indexOf("cluster:client"));
        assertThat(actual.indexOf("cluster:area")).isLessThan(actual.indexOf("cluster:client"));
        assertThat(actual.indexOf("neType:application")).isLessThan(actual.indexOf("cluster:application/group"));
    }

    @Test
    public void shouldReturnNewValue_whenValueDoesntExist() {
        Entity node = mock(Entity.class);
        when(node.getProperty("name")).thenReturn(null);

        assertThat(CsvTopologyLoader.getNewValue(node, "name", "#KEEP_VALUE_ELSE(myDefaultValue)")).isEqualTo("myDefaultValue");
    }

    @Test
    public void shouldNotReturnNewValue_whenValueExists() {
        Entity node = mock(Entity.class);
        when(node.getProperty("name")).thenReturn("someName");

        assertThat(CsvTopologyLoader.getNewValue(node, "name", "#KEEP_VALUE_ELSE(myDefaultValue)")).isNull();
    }
}