package com.livingobjects.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.export.query.Column;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.export.query.ExportQueryResult;
import com.livingobjects.neo4j.model.export.query.FullQuery;
import com.livingobjects.neo4j.model.export.query.Pair;
import com.livingobjects.neo4j.model.export.query.RelationshipQuery;
import com.livingobjects.neo4j.model.export.query.RelationshipQueryResult;
import com.livingobjects.neo4j.model.export.query.filter.Filter;
import com.livingobjects.neo4j.model.export.query.filter.ValueFilter;
import com.livingobjects.neo4j.rules.WithNeo4jImpermanentDatabase;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExportExtensionTest {

    private static final Filter<Column> NO_FILTER = new Filter.AndFilter<>(ImmutableList.of());
    private static final ImmutableSet<String> CLIENT_1_SCOPE = ImmutableSet.of("1");

    @Rule
    public WithNeo4jImpermanentDatabase wNeo = new WithNeo4jImpermanentDatabase()
            .withDatapacks("neo4j-test-database");

    private ExportExtension exportExtension;

    @Before
    public void setUp() {
        exportExtension = new ExportExtension(wNeo.getDatabaseManagementService(), Mockito.mock(Log.class));
    }

    @Test
    public void shouldFindAllCpeAndSiteWithAllProperties() {
        ExportQuery exportQuery = new ExportQuery(
                ImmutableSet.of("neType:cpe"),
                ImmutableSet.of("cluster:site"),
                ImmutableMap.of(),
                NO_FILTER,
                false,
                CLIENT_1_SCOPE,
                false,
                false);
        FullQuery fullQuery = new FullQuery(ImmutableList.of(exportQuery), null, null, null);

        ExportExtension.PaginatedLineages result = exportExtension.extract(fullQuery);

        Assertions.assertThat(result.total()).isEqualTo(7);

        Set<Pair<String, String>> cpeToSite = result.results().stream()
                .map(pair -> pair.first.get(0))
                .map(r -> new Pair<>(
                        (String) r.result.get("neType:cpe").get("name"),
                        (String) r.result.get("cluster:site").get("name")))
                .collect(Collectors.toSet());

        Assertions.assertThat(cpeToSite).containsExactlyInAnyOrder(
                new Pair<>("CC_RJ45", "Site_1"),
                new Pair<>("HW_3615", "Site_1"),
                new Pair<>("AA_RJ45", "Site_2"),
                new Pair<>("BB_3615", "Site_2"),
                new Pair<>("CPE_WITH_NO_WAN_LINKS", "Site_2"),
                new Pair<>("cpe6", "Site_4"),
                new Pair<>("NA_4233", "Site_5"));

        // Not restricting `columns` should still surface non-metadata properties beyond just tag/name/_type.
        ExportQueryResult ccRj45 = result.results().stream()
                .map(pair -> pair.first.get(0))
                .filter(r -> "CC_RJ45".equals(r.result.get("neType:cpe").get("name")))
                .findFirst()
                .orElseThrow();
        Assertions.assertThat(ccRj45.result.get("neType:cpe").get("ip")).isEqualTo("172.17.10.31");
    }

    @Test
    public void shouldFilterOnCpeNameAndReturnCpeAndSite() {
        Filter<Column> filter = new Filter.ColumnFilter<>(
                new Column("neType:cpe", "name"),
                new ValueFilter(false, ValueFilter.Operator.eq, "CC_RJ45"));
        ExportQuery exportQuery = new ExportQuery(
                ImmutableSet.of("neType:cpe"),
                ImmutableSet.of("cluster:site"),
                ImmutableMap.of(),
                filter,
                false,
                CLIENT_1_SCOPE,
                false,
                false);
        FullQuery fullQuery = new FullQuery(ImmutableList.of(exportQuery), null, null, null);

        ExportExtension.PaginatedLineages result = exportExtension.extract(fullQuery);

        Assertions.assertThat(result.total()).isEqualTo(1);
        ExportQueryResult row = result.results().get(0).first.get(0);
        Assertions.assertThat(row.result.get("neType:cpe").get("name")).isEqualTo("CC_RJ45");
        Assertions.assertThat(row.result.get("cluster:site").get("name")).isEqualTo("Site_1");
    }

    @Test
    public void shouldFindCommonWanLinkWhenFilteringCpeBySiteAndNetwork() {
        Filter<Column> filter = new Filter.AndFilter<>(ImmutableList.of(
                new Filter.ColumnFilter<>(new Column("cluster:site", "name"), new ValueFilter(false, ValueFilter.Operator.eq, "Site_4")),
                new Filter.ColumnFilter<>(new Column("cluster:network", "name"), new ValueFilter(false, ValueFilter.Operator.eq, "Network 03"))
        ));
        ExportQuery exportQuery = new ExportQuery(
                ImmutableSet.of("neType:cpe"),
                ImmutableSet.of(),
                ImmutableMap.of(),
                filter,
                false,
                CLIENT_1_SCOPE,
                false,
                false);
        FullQuery fullQuery = new FullQuery(ImmutableList.of(exportQuery), null, null, null);

        ExportExtension.PaginatedLineages result = exportExtension.extract(fullQuery);

        Assertions.assertThat(result.total()).isEqualTo(1);
        ExportQueryResult row = result.results().get(0).first.get(0);
        Assertions.assertThat(row.result.get("neType:cpe").get("name")).isEqualTo("cpe6");
    }

    @Test
    public void shouldReturnOnlyMatchingCpePairForIpslaCrossAttributeRelation() {
        Filter<Column> originFilter = new Filter.ColumnFilter<>(
                new Column("neType:cpe", "name"),
                new ValueFilter(false, ValueFilter.Operator.eq, "CC_RJ45"));
        ExportQuery originQuery = new ExportQuery(
                ImmutableSet.of("neType:cpe"),
                ImmutableSet.of(),
                ImmutableMap.of(),
                originFilter,
                false,
                CLIENT_1_SCOPE,
                false,
                false);
        ExportQuery destinationQuery = new ExportQuery(
                ImmutableSet.of("neType:cpe"),
                ImmutableSet.of(),
                ImmutableMap.of(),
                NO_FILTER,
                false,
                CLIENT_1_SCOPE,
                false,
                false);
        RelationshipQuery relationshipQuery = new RelationshipQuery(Direction.OUTGOING, "ipsla", ImmutableList.of("latencyMs"));
        FullQuery fullQuery = new FullQuery(
                ImmutableList.of(originQuery, destinationQuery),
                null,
                null,
                ImmutableList.of(relationshipQuery));

        ExportExtension.PaginatedLineages result = exportExtension.extract(fullQuery);

        Assertions.assertThat(result.total()).isEqualTo(1);
        Pair<List<ExportQueryResult>, List<RelationshipQueryResult>> row = result.results().get(0);
        Assertions.assertThat(row.first.get(0).result.get("neType:cpe").get("name")).isEqualTo("CC_RJ45");
        Assertions.assertThat(row.first.get(1).result.get("neType:cpe").get("name")).isEqualTo("HW_3615");
        Assertions.assertThat(row.second.get(0).result.get("latencyMs")).isEqualTo(15L);
    }
}
