package com.livingobjects.neo4j.loader;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.rules.WithNeo4jImpermanentDatabase;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class CsvTopologyLoaderTest {

    @Rule
    public WithNeo4jImpermanentDatabase wNeo = new WithNeo4jImpermanentDatabase()
            .withDatapacks("neo4j-test-database");
    private CsvTopologyLoader tested;

    @Before
    public void setUp() throws Exception {
        tested = new CsvTopologyLoader(wNeo.getGraphDatabaseService(), new MetricRegistry());
    }

    @Test
    public void shouldSortElementsToDelete() {
        List<String> actual;
        try (Transaction tx = wNeo.getGraphDatabaseService().beginTx()) {
            actual = tested.sortKeyTypes(ImmutableSet.of("cluster:client", "cluster:site", "neType:cpe","cluster:application/group",
                    "neType:viewpoint", "neType:wanLink", "cluster:area", "neType:application", "neType:cos"), tx);
        }
        Assertions.assertThat(actual.indexOf("neType:viewpoint")).isLessThan(actual.indexOf("neType:cpe"));
        Assertions.assertThat(actual.indexOf("neType:wanLink")).isLessThan(actual.indexOf("neType:cpe"));
        Assertions.assertThat(actual.indexOf("cluster:site")).isLessThan(actual.indexOf("cluster:client"));
        Assertions.assertThat(actual.indexOf("neType:cpe")).isLessThan(actual.indexOf("cluster:client"));
        Assertions.assertThat(actual.indexOf("cluster:area")).isLessThan(actual.indexOf("cluster:client"));
        Assertions.assertThat(actual.indexOf("neType:application")).isLessThan(actual.indexOf("cluster:application/group"));
    }
}