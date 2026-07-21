package com.livingobjects.neo4j;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.export.query.Column;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.export.query.FullQuery;
import com.livingobjects.neo4j.model.export.query.filter.Filter;
import com.livingobjects.neo4j.rules.WithNeo4jImpermanentDatabase;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.logging.Log;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Used to test performance on a big export query.
 * This test takes really long to complete (several minutes only for database loading).
 * This should remain Ignored and only be run manually, when testing the inventory performance after a change that might impact it significantly
 *
 * The query used returns wanLinks / Viewpoints on a topology of 500 clients, each having 800 sites and multiple cpe / viewpoint / wanLink for a total of ~2.2M lines returned
 *
 * To run this, you MUST add -Xmx16g (and optionally -Xms16g) to the "VM options" of the IDE running it (or use mvn -o test -Dtest=ExportExtensionPerformanceTest -DargLine="-Xmx12g")
 * During the test, close other RAM-hungry applications and monitor memory usage with htop (might swap which would impact performance)
 */
@Ignore
public class ExportExtensionPerformanceTest {

    private static final int CLIENT_COUNT = 1000;
    private static final int SITES_PER_CLIENT = 800;
    private static final int SITE_COUNT = CLIENT_COUNT * SITES_PER_CLIENT;

    @Rule
    public WithNeo4jImpermanentDatabase wNeo = new WithNeo4jImpermanentDatabase()
            .withDatapacks("neo4j-test-database")
            .withFixture(Paths.get("fixtures/performance"));

    @Test
    public void shouldExportBigWanLinkAndViewpointDatasetInReasonableTime() {
        ExportExtension extension = new ExportExtension(wNeo.getDatabaseManagementService(), Mockito.mock(Log.class));

        Filter<Column> noFilter = new Filter.AndFilter<>(ImmutableList.of());
        ExportQuery exportQuery = new ExportQuery(
                ImmutableSet.of("neType:wanLink", "neType:viewpoint"),
                ImmutableSet.of("neType:cpe", "cluster:site", "cluster:area", "cluster:network", "cluster:client"),
                ImmutableMap.of(),
                noFilter,
                false,
                ImmutableSet.of(),
                true,
                false);
        FullQuery fullQuery = new FullQuery(ImmutableList.of(exportQuery), null, null, null);

        Stopwatch stopwatch = Stopwatch.createStarted();
        ExportExtension.PaginatedLineages result = extension.extract(fullQuery);
        long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("ExportExtension.extract() took " + elapsedMs + " ms for " + result.total() + " lines "
                + "(" + SITE_COUNT + " wanLinks + " + SITE_COUNT + " viewpoints in the source dataset).");
    }
}
