package com.livingobjects.neo4j;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.livingobjects.neo4j.helper.PlanetByContext;
import com.livingobjects.neo4j.helper.PlanetFactory;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.CrossRelationship;
import com.livingobjects.neo4j.model.export.Lineage;
import com.livingobjects.neo4j.model.export.LineageListNaturalComparator;
import com.livingobjects.neo4j.model.export.LineageListSortComparator;
import com.livingobjects.neo4j.model.export.Lineages;
import com.livingobjects.neo4j.model.export.PropertyDefinition;
import com.livingobjects.neo4j.model.export.PropertyNameComparator;
import com.livingobjects.neo4j.model.export.query.Column;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.export.query.ExportQueryResult;
import com.livingobjects.neo4j.model.export.query.FullQuery;
import com.livingobjects.neo4j.model.export.query.Pagination;
import com.livingobjects.neo4j.model.export.query.Pair;
import com.livingobjects.neo4j.model.export.query.RelationshipQuery;
import com.livingobjects.neo4j.model.export.query.RelationshipQueryResult;
import com.livingobjects.neo4j.model.export.query.filter.Filter;
import com.livingobjects.neo4j.model.export.query.filter.ValueFilter;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import com.opencsv.CSVWriter;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Charsets.UTF_8;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.CONNECT;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.CROSS_ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.EXTEND;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/export")
public final class ExportExtension {

    private static final MediaType TEXT_CSV_MEDIATYPE = MediaType.valueOf("text/csv");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportExtension.class);

    private final ObjectMapper json = new ObjectMapper();

    private final GraphDatabaseService graphDb;
    private final TemplatedPlanetFactory templatedPlanetFactory;
    private final PlanetFactory planetFactory;
    private final MetaSchema metaSchema;

    public ExportExtension(@Context DatabaseManagementService dbms) {
        this.graphDb = dbms.database(dbms.listDatabases().get(0));
        this.templatedPlanetFactory = new TemplatedPlanetFactory(graphDb);
        this.planetFactory = new PlanetFactory(graphDb);
        try (Transaction tx = graphDb.beginTx()) {
            this.metaSchema = new MetaSchema(tx);
        }
    }

    @GET
    @Path("/properties")
    public Response exportProperties() throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, Map<String, PropertyDefinition>> allProperties = Maps.newHashMap();
            tx.findNodes(Labels.ELEMENT)
                    .forEachRemaining(node -> {
                        String type = node.getProperty(_TYPE).toString();
                        Map<String, PropertyDefinition> properties = allProperties
                                .computeIfAbsent(type, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
                        node.getAllProperties()
                                .forEach((name, value) -> {
                                    if (!name.startsWith("_") && !properties.containsKey(name)) {
                                        boolean required = isRequired(type, name, metaSchema);
                                        properties.put(name, new PropertyDefinition(PropertyConverter.getPropertyType(value), required));
                                    }
                                });
                    });
            return Response.ok()
                    .entity(JSON_MAPPER.writeValueAsString(allProperties))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .build();
        } catch (Throwable e) {
            LOGGER.error("export properties extension : ", e);
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Response.Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();
        }
    }

    @POST
    @Path("/count")
    public Response export(InputStream in) throws IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        try {
            FullQuery query = json.readValue(in, new TypeReference<>() {
            });

            PaginatedLineages lineages = extract(query);

            return Response.ok()
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .entity(JSON_MAPPER.writeValueAsString(lineages.total()))
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.error("export extension : ", e);
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Response.Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();

        } catch (Exception e) {
            LOGGER.error("export extension : ", e);
            if (e.getCause() != null) {
                return errorResponse(e.getCause());
            } else {
                return errorResponse(e);
            }

        } finally {
            LOGGER.info("Export count in {} ms.", stopWatch.elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @POST
    public Response export(InputStream in, @HeaderParam("accept") String accept) throws IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();

        try {
            FullQuery query = json.readValue(in, new TypeReference<>() {
            });
            boolean csv = checkAcceptHeader(accept);

            PaginatedLineages lineages = extract(query);
            StreamingOutput stream;
            MediaType mediatype;
            if (csv) {
                stream = outputStream -> exportAsCsv(lineages, outputStream);
                mediatype = TEXT_CSV_MEDIATYPE;
            } else {
                stream = outputStream -> exportAsJson(lineages, outputStream);
                mediatype = MediaType.APPLICATION_JSON_TYPE;
            }

            return Response.ok()
                    .header("Content-Range", "" + lineages.start() + '-' + lineages.end() + '/' + lineages.total())
                    .entity(stream)
                    .type(mediatype)
                    .build();

        } catch (IllegalArgumentException e) {
            LOGGER.error("export extension : ", e);
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Response.Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();

        } catch (Exception e) {
            LOGGER.error("export extension : ", e);
            if (e.getCause() != null) {
                return errorResponse(e.getCause());
            } else {
                return errorResponse(e);
            }

        } finally {
            LOGGER.info("Export in {} ms.", stopWatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private boolean isRequired(String type, String s, MetaSchema metaSchema) {
        if (ImmutableSet.of("tag", "id").contains(s)) {
            return true;
        } else {
            return metaSchema.getRequiredProperties(type).contains(s);
        }
    }

    private boolean checkAcceptHeader(@HeaderParam("accept") String accept) {
        boolean csv;
        if (TEXT_CSV_MEDIATYPE.equals(MediaType.valueOf(accept))) {
            csv = true;
        } else if (MediaType.APPLICATION_JSON_TYPE.equals(MediaType.valueOf(accept))) {
            csv = false;
        } else {
            throw new IllegalArgumentException("'" + accept + "' content type is not supported. Use 'text/csv' or 'application/json'");
        }
        return csv;
    }

    private PaginatedLineages paginate(List<Lineages> lineages,
                                       List<Pair<List<Lineage>, List<Map<String, Object>>>> sortedLines,
                                       Optional<Pagination> pagination) {
        if (pagination.isPresent() && pagination.get().offset > sortedLines.size()) {
            return EMPTY_PAGINATED_LINEAGE;
        }
        int end = pagination
                .map(p -> Math.min(p.offset + p.limit, sortedLines.size()))
                .orElse(sortedLines.size());
        return new PaginatedLineages() {

            private List<Pair<List<Lineage>, List<Map<String, Object>>>> lineages() {
                return pagination
                        .map(p -> sortedLines.subList(p.offset, end))
                        .orElse(sortedLines);
            }

            @Override
            public ImmutableSet<String> attributesToExport() {
                if (lineages.size() > 1) {
                    throw new UnsupportedOperationException("Queries with relationships can't be exported as csv");
                }
                return lineages.get(0).attributesToExport;
            }

            @Override
            public Map<String, SortedMap<String, String>> header() {
                if (lineages.size() > 1) {
                    throw new UnsupportedOperationException("Queries with relationships can't be exported as csv");
                }
                return lineages.get(0).propertiesTypeByType;
            }

            @Override
            public List<Pair<List<ExportQueryResult>, List<RelationshipQueryResult>>> results() {
                return lineages()
                        .stream()
                        .map(lineagesAndRelProps ->
                                new Pair<>(
                                        Lists.transform(lineagesAndRelProps.first, l -> l == null ?
                                                new ExportQueryResult(ImmutableMap.of()) :
                                                new ExportQueryResult(ImmutableMap.copyOf(l.propertiesToExportByType))),
                                        Lists.transform(lineagesAndRelProps.second, props -> props == null ?
                                                new RelationshipQueryResult(ImmutableMap.of()) :
                                                new RelationshipQueryResult(ImmutableMap.copyOf(props)))
                                )
                        ).collect(Collectors.toList());
            }

            @Override
            public int start() {
                return pagination
                        .map(p -> p.offset)
                        .orElse(0);
            }

            @Override
            public int end() {
                return end;
            }

            @Override
            public int total() {
                return sortedLines.size();
            }
        };
    }

    /**
     * Adds to lineages.propertiesTypeByType all the properties from given lineage
     */
    private void initializePropertiesList(Lineages lineages, Lineage lineage) {
        for (String keyAttribute : lineages.attributesToExport) {
            SortedMap<String, String> propertiesType = lineages.getKeyAttributePropertiesType(keyAttribute);
            lineages.getPropertiesToExport(keyAttribute)
                    .map(elements -> (List<String>) Lists.newArrayList(elements))
                    .orElseGet(() -> lineage.getAllPropertiesForType(keyAttribute))
                    .stream()
                    .filter(property -> !lineages.ignoreProperty(property) && lineages.filterColumn(keyAttribute, property))
                    .forEach(property -> propertiesType.putIfAbsent(property, PropertyConverter.getPropertyType(property)));
        }
    }

    /**
     * Fills lineage.propertiesToExportByType with all the properties to export from this lineage
     */
    private void initializePropertiesToExport(Lineages lineages, Lineage lineage) {
        for (String keyAttribute : lineages.attributesToExport) {
            Map<String, Object> values = lineage.propertiesToExportByType
                    .computeIfAbsent(keyAttribute, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
            List<String> propertiesToExport = Lists.newArrayList(lineages.propertiesTypeByType.get(keyAttribute).keySet());
            for (String property : propertiesToExport) {
                if (!lineages.ignoreProperty(property)) {
                    values.put(property, lineage.getProperty(keyAttribute, property));
                }
            }
        }
    }

    private PaginatedLineages extract(FullQuery initQuery) {
        try (Transaction tx = graphDb.beginTx()) {
            MetaSchema metaSchema = new MetaSchema(tx);

            // to be sure we isolate scopes, always add a filter on scope for each individual query
            List<ExportQuery> queriesWithScopeFilter = initQuery.exportQueries.stream()
                    .map(q -> {
                        // any requiredAttributes is neither global or sp
                        boolean isClientScope = q.requiredAttributes.stream()
                                .map(attr -> metaSchema.getAuthorizedScopes(tx, attr))
                                .anyMatch(scope -> !scope.contains(SCOPE_GLOBAL_ATTRIBUTE));
                        if (q.scopes.isEmpty() || !isClientScope) {
                            return q;
                        }

                        Set<String> initScopeFilters = getScopeFilters(q);
                        List<Filter<Column>> scopeFilters = q.scopes.stream()
                                // remove global scope
                                .filter(s -> !s.equals(GLOBAL_SCOPE.id) && !s.equals(SP_SCOPE.id))
                                // remove scopes already filtered
                                .filter(f -> !initScopeFilters.contains(f))
                                .map(s -> new Filter.ColumnFilter<>(new Column("cluster:client", ID), new ValueFilter(false, ValueFilter.Operator.eq, s)))
                                .collect(Collectors.toList());

                        Filter<Column> filterWithScopes = scopeFilters.isEmpty() ? q.filter :
                                new Filter.AndFilter<>(Lists.newArrayList(q.filter, new Filter.OrFilter<>(scopeFilters)));
                        return new ExportQuery(q.requiredAttributes,
                                q.parentAttributes,
                                q.columns,
                                filterWithScopes,
                                q.includeMetadata, q.scopes, q.parentsCardinality, q.noResult);
                    }).collect(Collectors.toList());
            FullQuery fullQuery = new FullQuery(queriesWithScopeFilter,
                    initQuery.pagination.orElse(null),
                    initQuery.ordersByIndex,
                    initQuery.relationshipQueries);

            List<CrossRelationship> relations = fullQuery.relationshipQueries.stream()
                    .map(rq -> metaSchema.getRelationshipOfType(rq.type))
                    .collect(Collectors.toList());
            List<Lineages> lineages = exportLineages(fullQuery, relations, tx);
            List<List<Lineage>> filteredLineages = Lists.newArrayList();
            for (int i = 0; i < lineages.size(); i++) {
                List<Lineage> filter = filter(fullQuery.exportQueries.get(i), lineages.get(i));
                filteredLineages.add(filter);
                for (Lineage l : filter) {
                    initializePropertiesList(lineages.get(i), l);
                }
                for (Lineage l : filter) {
                    initializePropertiesToExport(lineages.get(i), l);
                }
            }
            List<Pair<List<Lineage>, List<Map<String, Object>>>> filteredLines = constructLines(
                    lineages,
                    filteredLineages,
                    fullQuery.relationshipQueries,
                    ImmutableList.copyOf(relations),
                    fullQuery.exportQueries
            );

            List<String> attrs = lineages.stream()
                    .map(l -> l.attributesToExtract)
                    .collect(Lists::newArrayList,
                            ArrayList::addAll,
                            (a, b) -> {
                                throw new UnsupportedOperationException("Parallel stream is not supported here");
                            }
                    );

            ImmutableSet<String> attributesOrdering = ImmutableSet.copyOf(attrs);
            Comparator<List<Lineage>> lineageSortComparator = new LineageListSortComparator(
                    fullQuery.ordersByIndex,
                    new LineageListNaturalComparator(attributesOrdering)
            );
            filteredLines.sort((o1, o2) -> lineageSortComparator.compare(o1.first, o2.first));
            return paginate(lineages, filteredLines, fullQuery.pagination);
        }
    }

    /**
     * returns a Stream of all Scope filters of the query
     */
    private Set<String> getScopeFilters(ExportQuery q) {
        ImmutableList<Filter.ColumnFilter<Column>> columnFilters = q.filter.columnsFilters();
        return columnFilters == null || columnFilters.isEmpty() ? ImmutableSet.of() : columnFilters.stream()
                .filter(columnColumnFilter -> metaSchema.isScope(columnColumnFilter.column.keyAttribute))
                .filter(columnFilter -> columnFilter.column.property.equals(ID))
                .filter(columnFilter -> columnFilter.valueFilter.operator.equals(ValueFilter.Operator.eq) &&
                        !columnFilter.valueFilter.not)
                .map(columnFilter -> columnFilter.valueFilter.value.toString())
                .collect(Collectors.toSet());
    }

    /**
     * Returns all the tuples of Lineages linked by relationships, and the requested properties of the relationship
     * Duplicate lines will be removed (two lines are duplicate if all the attributes and relations requested have the same id)
     */
    private List<Pair<List<Lineage>, List<Map<String, Object>>>> constructLines(List<Lineages> metaLineages,
                                                                                List<List<Lineage>> lineages,
                                                                                ImmutableList<RelationshipQuery> relationQueries,
                                                                                ImmutableList<CrossRelationship> metaRelations,
                                                                                ImmutableList<ExportQuery> exportQueries) {
        for (List<Lineage> l : lineages) {
            if (l.isEmpty()) {
                return Lists.newArrayList();
            }
        }
        List<Pair<List<Lineage>, List<Relationship>>> lines = Lists.newArrayList();
        constructLinesR(lines,
                ImmutableList.copyOf(lineages.stream()
                        .map(ImmutableList::copyOf)
                        .collect(Collectors.toList())
                ),
                relationQueries,
                metaRelations);

        // replace useless lineages with empty lineage
        for (int i = 0; i < exportQueries.size(); i++) {
            if (exportQueries.get(i).noResult) {
                int finalI = i;
                lines.forEach(l -> l.first.set(finalI, new Lineage((GraphDatabaseService) null)));
            }
        }

        // Removes duplicate lines
        List<Integer> duplicateLineIndexes = Lists.newArrayList();
        Set<List<Long>> alreadySeenLines = Sets.newHashSet();
        for (int lineIdx = 0; lineIdx < lines.size(); lineIdx++) {
            Pair<List<Lineage>, List<Relationship>> line = lines.get(lineIdx);
            List<Long> lineIds = Lists.newArrayList();
            // get ids of nodes
            for (int i = 0; i < line.first.size(); i++) {
                Lineages metaLineage = metaLineages.get(i);
                Lineage lineage = line.first.get(i);
                lineIds.addAll(
                        metaLineage.attributesToExport.stream()
                                .map(keyAttribute -> Optional.ofNullable(lineage.nodesByType.get(keyAttribute))
                                        .map(Node::getId)
                                        .orElse(-1L))
                                .collect(Collectors.toList())
                );
            }

            // get ids of relationships
            for (int i = 0; i < line.second.size(); i++) {
                Relationship relationship = line.second.get(i);
                List<String> propertiesToExport = relationQueries.get(i).propertiesToExport;
                if (propertiesToExport != null && !propertiesToExport.isEmpty()) {
                    lineIds.add(relationship.getId());
                }
            }

            if (!alreadySeenLines.add(lineIds)) {
                duplicateLineIndexes.add(lineIdx);
            }
        }

        // remove higher indexes first
        Lists.reverse(duplicateLineIndexes)
                .forEach(i -> lines.remove((int) i));

        // Replace Relationship by Map<String, Object>, only keeping requested properties
        return lines.stream()
                .map(pair -> {
                    List<Map<String, Object>> relationsProperties = Lists.newArrayList();
                    for (int i = 0; i < pair.second.size(); i++) {
                        Relationship r = pair.second.get(i);
                        RelationshipQuery relationQuery = relationQueries.get(i);
                        relationsProperties.add(
                                relationQuery.propertiesToExport.stream()
                                        .map(property -> {
                                            try {
                                                return Maps.immutableEntry(property, r.getProperty(property));
                                            } catch (NotFoundException e) {
                                                LOGGER.warn(String.format("Requested property %s could not be found on relation %s, " +
                                                        "empty value will be returned instead", property, r));
                                                return Maps.immutableEntry(property, "");
                                            }
                                        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                        );
                    }
                    return new Pair<>(pair.first, relationsProperties);
                })
                .collect(Collectors.toList());
    }

    /**
     * Given allLineages, recursively fills lines. Each List inside will contain one and only one lineage from each List inside allLineages,
     * with two consecutive lineages being linked by a relationship described in relationQueries and metaRelations.
     * lines[i] and lines[i+1] are linked by the relationship described by relationQueries[i] and metaRelations[i]
     */
    private void constructLinesR(
            List<Pair<List<Lineage>, List<Relationship>>> lines,
            ImmutableList<ImmutableList<Lineage>> allLineages,
            ImmutableList<RelationshipQuery> relationQueries,
            ImmutableList<CrossRelationship> metaRelations
    ) {
        if (lines.isEmpty()) {
            allLineages.get(0).forEach(l -> lines.add(new Pair<>(Lists.newArrayList(l), Lists.newArrayList())));
            constructLinesR(lines, allLineages, relationQueries, metaRelations);
            return;
        }
        int i = lines.get(0).first.size() - 1;
        if (allLineages.size() == i + 1) {
            return;
        }
        List<Lineage> nextLineages = allLineages.get(i + 1);
        ImmutableList.copyOf(lines)
                .forEach(pair -> {
                    Lineage l = pair.first.get(i);
                    List<Pair<Lineage, Relationship>> toAdd = getMatchingLineageAndRelationProperties(l,
                            nextLineages,
                            metaRelations.get(i),
                            relationQueries.get(i)
                    );
                    lines.remove(pair);
                    lines.addAll(addNext(pair, toAdd));
                });
        if (!lines.isEmpty()) {
            constructLinesR(lines, allLineages, relationQueries, metaRelations);
        }
    }

    private List<Pair<List<Lineage>, List<Relationship>>> addNext(
            Pair<List<Lineage>, List<Relationship>> initial,
            List<Pair<Lineage, Relationship>> next
    ) {
        return next.stream()
                .map(lineageMapPair -> {
                    List<Lineage> newLineages = Lists.newArrayList(initial.first);
                    List<Relationship> newRelationProperties = Lists.newArrayList(initial.second);
                    newRelationProperties.add(lineageMapPair.second);
                    newLineages.add(lineageMapPair.first);
                    return new Pair<>(newLineages, newRelationProperties);
                }).collect(Collectors.toList());
    }

    private List<Pair<Lineage, Relationship>> getMatchingLineageAndRelationProperties(Lineage originLineage,
                                                                                      List<Lineage> destLineage,
                                                                                      CrossRelationship metaRelation,
                                                                                      RelationshipQuery relationQuery) {
        Direction direction = relationQuery.direction;
        String originType = direction == INCOMING ? metaRelation.originType : metaRelation.destinationType;
        String destType = direction == INCOMING ? metaRelation.destinationType : metaRelation.originType;
        return Streams.stream(originLineage.nodesByType.get(originType).getRelationships(direction, CROSS_ATTRIBUTE))
                .map(r -> {
                    Node n = direction == INCOMING ? r.getStartNode() : r.getEndNode();
                    return new Pair<>(n, r);
                })
                .flatMap(pair -> destLineage.stream()
                        .filter(l -> l.nodesByType.get(destType).equals(pair.first))
                        .map(lineage -> new Pair<>(lineage, pair.second)))
                .collect(Collectors.toList());
    }

    private void exportAsCsv(PaginatedLineages paginatedLineages, OutputStream outputStream) {

        try (CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(outputStream))) {
//        try (AutoCloseableCsvWriter csv = Csv.write().to(outputStream).autoClose()) {
            if (paginatedLineages.results() != null &&
                    paginatedLineages.results()
                            .stream()
                            .map(pair -> pair.first)
                            .map(List::size)
                            .max(Comparator.naturalOrder())
                            .orElse(0) > 1) {
                throw new UnsupportedOperationException("Queries with relationships can't be exported as csv");
            }
            // If there is no lineage to write, don't even write the headers
            if (paginatedLineages.results().size() > 0 && paginatedLineages.results().get(0).first.size() > 0) {
                String[] headers = generateCSVHeader(paginatedLineages);
                if (headers.length > 0) {
                    csvWriter.writeNext(headers, false);
                    csvWriter.flush();
                }
                paginatedLineages.results()
                        .stream()
                        .map(list -> list.first.get(0))
                        .forEach(filteredLineage -> writeCSVLine(filteredLineage, csvWriter));
            }
            csvWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Lineage> filter(ExportQuery exportQuery, Lineages lineages) {
        return ImmutableList.copyOf(lineages.lineages().stream()
                .map(lineage -> filterLineage(exportQuery, lineages, lineage))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()));
    }

    private void exportAsJson(PaginatedLineages paginatedLineages, OutputStream outputStream) {
        try {

            for (Pair<List<ExportQueryResult>, List<RelationshipQueryResult>> results : paginatedLineages.results()) {
                String s = JSON_MAPPER.writeValueAsString(results);
                outputStream.write(s.getBytes(UTF_8));
                outputStream.write('\n');
            }
        } catch (Throwable e) {
            LOGGER.error("export extension : ", e);
            throw new RuntimeException(e);
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOGGER.error("Error occured while closign outputStream: ", e);
            }
        }
    }

    private ImmutableList<Lineages> exportLineages(FullQuery fullQuery, List<CrossRelationship> relations, Transaction tx) {
        ImmutableList.Builder<Lineages> lineagesBuilder = ImmutableList.builder();
        for (int i = 0; i < fullQuery.exportQueries.size(); i++) {
            ExportQuery exportQuery = fullQuery.exportQueries.get(i);
            ImmutableSet.Builder<String> requiredChildren = ImmutableSet.<String>builder()
                    .addAll(getCommonChildren(exportQuery.requiredAttributes, metaSchema));
            Optional<RelationshipQuery> previousQuery = i == 0 ? Optional.empty() : Optional.of(fullQuery.relationshipQueries.get(i - 1));
            Optional<RelationshipQuery> nextQuery = i == fullQuery.relationshipQueries.size() ?
                    Optional.empty() :
                    Optional.of(fullQuery.relationshipQueries.get(i));

            Function<RelationshipQuery, Pair<RelationshipQuery, CrossRelationship>> queryToPair =
                    q -> new Pair<>(q, relations.stream().filter(r -> r.type.equals(q.type)).collect(MoreCollectors.onlyElement()));
            Optional<Pair<RelationshipQuery, CrossRelationship>> previousRel = previousQuery.map(queryToPair);
            Optional<Pair<RelationshipQuery, CrossRelationship>> nextRel = nextQuery.map(queryToPair);

            previousRel.ifPresent(q -> requiredChildren.add(q.first.direction == INCOMING ? q.second.originType : q.second.destinationType));
            nextRel.ifPresent(q -> requiredChildren.add(q.first.direction == INCOMING ? q.second.destinationType : q.second.originType));

            lineagesBuilder.add(exportLineagesSingleQuery(requiredChildren.build(), exportQuery, previousRel, nextRel, tx));
        }
        return lineagesBuilder.build();
    }

    private Lineages exportLineagesSingleQuery(ImmutableSet<String> requiredCommonChildren,
                                               ExportQuery exportQuery,
                                               Optional<Pair<RelationshipQuery, CrossRelationship>> previousRel,
                                               Optional<Pair<RelationshipQuery, CrossRelationship>> nextRel,
                                               Transaction tx) {
        List<String> filterAttributes = exportQuery.filter.columns()
                .stream()
                .map(c -> c.keyAttribute)
                .collect(Collectors.toList());
        Set<List<String>> allCombinations;
        if (exportQuery.requiredAttributes.isEmpty()) {
            allCombinations = ImmutableSet.of(filterAttributes);
        } else if (filterAttributes.isEmpty()) {
            allCombinations = ImmutableSet.of(ImmutableList.copyOf(exportQuery.requiredAttributes));
        } else {
            allCombinations = Sets.cartesianProduct(
                    exportQuery.requiredAttributes,
                    ImmutableSet.copyOf(filterAttributes)
            );
        }
        ImmutableSet<String> filterCommonChildren = allCombinations
                .stream()
                .map(ImmutableSet::copyOf)
                .map(types -> getCommonChildren(types, metaSchema))
                .reduce((set, set2) -> Sets.union(set, set2).immutableCopy())
                .orElseGet(ImmutableSet::of);
        ImmutableList.Builder<String> relationAttrToExport = ImmutableList.builder();
        previousRel.ifPresent(pair -> relationAttrToExport.add(pair.first.direction == INCOMING ? pair.second.originType : pair.second.destinationType));
        nextRel.ifPresent(pair -> relationAttrToExport.add(pair.first.direction == INCOMING ? pair.second.destinationType : pair.second.originType));
        Lineages lineages = initLineages(exportQuery, Sets.union(requiredCommonChildren, filterCommonChildren).immutableCopy(),
                relationAttrToExport.build(), tx);
        if (!lineages.attributesToExport.isEmpty() || lineages.noResult) {
            for (String leafAttribute : lineages.orderedLeafAttributes) {
                getNodeIterator(leafAttribute, exportQuery, tx)
                        .filter(nodeFilter(leafAttribute, previousRel, nextRel, tx))
                        .forEach(leaf -> {
                            if (!lineages.dejaVu(leaf)) {
                                Lineage lineage = new Lineage(graphDb);
                                if (!lineages.attributesToExtract.isEmpty()) {
                                    rewindLineage(leaf, lineages)
                                            .forEach(lineages::add);
                                } else {
                                    // no attributesToExport => this is just used for relationships
                                    lineage.nodesByType.put(leaf.getProperty(_TYPE, "").toString(), leaf);
                                    lineages.add(lineage);
                                }
                            }
                        });
            }
        }
        return lineages;
    }

    /**
     * returns a predicate telling whether a node of the given nodeType has a parent of each given type with a relationship matching the query
     *
     * @param nodeType    type of initial node
     * @param previousRel Relationship with the previous Lineage (in the order defined by the original query), the direction of this relationship is reversed
     * @return The predicate
     */
    private Predicate<Node> nodeFilter(String nodeType,
                                       Optional<Pair<RelationshipQuery, CrossRelationship>> previousRel,
                                       Optional<Pair<RelationshipQuery, CrossRelationship>> nextRel,
                                       Transaction tx) {
        return node -> {
            AtomicBoolean result = new AtomicBoolean(true);
            // check if previousRel is satisfied
            previousRel.ifPresent(pair -> {
                String destType = pair.first.direction == INCOMING ? pair.second.originType : pair.second.destinationType;
                ImmutableList<ImmutableList<String>> upwardPaths = metaSchema.getUpwardPath(tx, nodeType, destType);
                AtomicBoolean relRes = new AtomicBoolean(false);
                for (ImmutableList<String> path : upwardPaths) {
                    Optional<Node> optParent = getNodeFromPath(node, path);
                    optParent.ifPresent(parent -> relRes.set(relRes.get() ||
                            Streams.stream(parent.getRelationships(pair.first.direction == INCOMING ? OUTGOING : INCOMING, CROSS_ATTRIBUTE))
                                    .anyMatch(r -> pair.first.type.equals(r.getProperty(_TYPE).toString()))));
                }
                result.set(result.get() && relRes.get());
            });

            // check if nextRel is satisfied
            nextRel.ifPresent(pair -> {
                String destType = pair.first.direction == INCOMING ? pair.second.destinationType : pair.second.originType;
                ImmutableList<ImmutableList<String>> upwardPaths = metaSchema.getUpwardPath(tx, nodeType, destType);
                AtomicBoolean relRes = new AtomicBoolean(false);
                for (ImmutableList<String> path : upwardPaths) {
                    Optional<Node> optParent = getNodeFromPath(node, path);
                    optParent.ifPresent(parent -> relRes.set(relRes.get() ||
                            Streams.stream(parent.getRelationships(pair.first.direction, CROSS_ATTRIBUTE))
                                    .anyMatch(r -> pair.first.type.equals(r.getProperty(_TYPE).toString()))));
                }
                result.set(result.get() && relRes.get());
            });

            return result.get();
        };
    }

    private Optional<Node> getNodeFromPath(Node initialNode, ImmutableList<String> path) {
        return path.size() == 0 ?
                Optional.of(initialNode) :
                Streams.stream(initialNode.getRelationships(OUTGOING, CONNECT).iterator())
                        .map(Relationship::getEndNode)
                        .filter(node -> node.getProperty(_TYPE).equals(path.get(0)))
                        .findAny()
                        .flatMap(node -> getNodeFromPath(node, path.subList(1, path.size())));
    }


    /**
     * For a set of types, gives their common children of higher rank. If a common child has childs himself, those won't be returned
     */
    private ImmutableSet<String> getCommonChildren(ImmutableSet<String> types, MetaSchema metaSchema) {
        List<ImmutableSet<ImmutableList<String>>> allLineages = types.stream()
                .map(type -> metaSchema.getMetaLineagesForType(type)
                        .stream()
                        .map(lineage -> lineage.subList(0, lineage.indexOf(type) + 1))
                        .collect(Collectors.toList()))
                .map(ImmutableSet::copyOf)
                .collect(Collectors.toList());
        HashSet<String> result = Sets.cartesianProduct(allLineages).stream()
                .map(ImmutableSet::copyOf)
                .map(lineages -> lineages.stream()
                        .reduce((attrs1, attrs2) -> ImmutableList.copyOf(Sets.intersection(new HashSet<>(attrs1), new HashSet<>(attrs2)).immutableCopy()))
                        .orElseGet(ImmutableList::of))
                .flatMap(Collection::stream)
                .distinct() // all distinct common elements
                .reduce(new HashSet<>(), (uniqueChilds, currentChild) -> {
                            boolean match = false;
                            ImmutableList<ImmutableList<String>> metaLineagesForType = metaSchema.getMetaLineagesForType(currentChild);
                            for (ImmutableList<String> lineage : metaLineagesForType) {
                                for (String child : uniqueChilds) {
                                    if (lineage.contains(child)) {
                                        match = true;
                                        if (lineage.indexOf(currentChild) > lineage.indexOf(child)) {
                                            uniqueChilds.remove(child);
                                            uniqueChilds.add(currentChild);
                                        }
                                    }
                                }
                            }
                            if (!match) {
                                uniqueChilds.add(currentChild);
                            }
                            return uniqueChilds;
                        },
                        (strings, strings2) -> {
                            throw new UnsupportedOperationException("Merging of sets is not allowed for this reduction");
                        });
        return ImmutableSet.copyOf(result);
    }

    private Stream<Node> getNodeIterator(String leafAttribute, ExportQuery exportQuery, Transaction tx) {
        Set<String> initScopes = !exportQuery.scopes.isEmpty() ? exportQuery.scopes :
                getScopeFilters(exportQuery);

        ImmutableSet<String> scopes = getApplicableScopes(initScopes);

        if (scopes.isEmpty()) {
            return tx.findNodes(Labels.ELEMENT, GraphModelConstants._TYPE, leafAttribute).stream();
        } else {
            if (metaSchema.isOverridable(leafAttribute)) {
                ImmutableSet<String> allPlanets = templatedPlanetFactory.getPlanetByContext(leafAttribute).allPlanets();
                Set<Node> authorizedPlanets = scopes.stream()
                        .flatMap(scopeId -> {
                            List<String> possibleScopes;
                            if (scopeId.equals(GLOBAL_SCOPE.id)) {
                                possibleScopes = Lists.newArrayList(GLOBAL_SCOPE.id);
                            } else if (scopeId.equals(SP_SCOPE.id)) {
                                possibleScopes = Lists.newArrayList(SP_SCOPE.id, GLOBAL_SCOPE.id);
                            } else {
                                possibleScopes = Lists.newArrayList(scopeId, SP_SCOPE.id, GLOBAL_SCOPE.id);
                            }
                            return possibleScopes.stream()
                                    .flatMap(scope -> allPlanets.stream()
                                            .map(planetTemplate -> planetFactory.get(planetTemplate, scope, tx))
                                            .filter(Objects::nonNull));
                        }).collect(Collectors.toSet());
                return authorizedPlanets.stream()
                        .flatMap(planetNode -> StreamSupport.stream(planetNode.getRelationships(INCOMING, RelationshipTypes.ATTRIBUTE).spliterator(), false)
                                .map(Relationship::getStartNode)) // All nodes which do not extend another
                        .map(node -> getOverridingNode(node, authorizedPlanets));
            } else {
                PlanetByContext planetByContext = templatedPlanetFactory.getPlanetByContext(leafAttribute);
                return scopes.stream()
                        .flatMap(scopeId ->
                                planetByContext.allPlanets().stream()
                                        .map(planetTemplate -> planetFactory.get(planetTemplate, scopeId, tx))
                                        .filter(Objects::nonNull)
                                        .flatMap(planetNode -> StreamSupport.stream(planetNode.getRelationships(INCOMING, ATTRIBUTE).spliterator(), false)
                                                .map(Relationship::getStartNode))
                                        .filter(node -> !node.getRelationships(OUTGOING, EXTEND).iterator().hasNext()));
            }
        }
    }

    /**
     * Gets all the scopes applicable to the given set of scopes.
     * If scopes contains at least one client Scope, return all scopes, plus SP and GLOBAL
     * If scopes contains SP, return SP and GLOBAL
     * If scope only contains GLOBAL, return GLOBAL
     */
    private ImmutableSet<String> getApplicableScopes(Set<String> initialScopes) {
        if (initialScopes.isEmpty()) {
            return ImmutableSet.of();
        }
        return initialScopes.contains(GLOBAL_SCOPE.id) && initialScopes.size() == 1 ?
                ImmutableSet.copyOf(initialScopes) :
                ImmutableSet.<String>builder().addAll(initialScopes).add(SP_SCOPE.id, GLOBAL_SCOPE.id).build();
    }

    private Node getOverridingNode(Node element, Set<Node> authorizedPlanets) {
        return StreamSupport.stream(element.getRelationships(INCOMING, EXTEND).spliterator(), false)
                .map(Relationship::getStartNode)
                .filter(node -> authorizedPlanets.contains(Lists.newArrayList(node.getRelationships(OUTGOING, ATTRIBUTE)).get(0).getEndNode()))
                .findAny()
                .map(node -> getOverridingNode(node, authorizedPlanets))
                .orElse(element);
    }

    private Lineages initLineages(ExportQuery exportQuery, Set<String> commonChildren,
                                  ImmutableList<String> relAttrToExport, Transaction tx) {
        return new Lineages(tx, metaSchema, exportQuery, commonChildren, relAttrToExport);
    }

    private Set<Lineage> rewindLineage(Node currentNode, Lineages lineages) {
        return doRewind(currentNode, new HashMap<>(), lineages).stream()
                .map(nodesByType -> {
                    Lineage lineage = new Lineage(graphDb);
                    lineage.nodesByType.putAll(nodesByType);
                    return lineage;
                }).collect(ImmutableSet.toImmutableSet());
    }

    private Set<Map<String, Node>> doRewind(Node currentNode, Map<String, Node> nodesByType, Lineages lineages) {
        String type = currentNode.getProperty(_TYPE, "").toString();
        if (lineages.attributesToExtract.contains(type)) {
            nodesByType.put(type, currentNode);
        }
        lineages.markAsVisited(currentNode);
        if (nodesByType.keySet().containsAll(lineages.attributesToExtract)) {
            return ImmutableSet.of(nodesByType);
        }
        Iterable<Relationship> parentRelationships = currentNode.getRelationships(OUTGOING, CONNECT);

        ImmutableSetMultimap.Builder<String, Node> parentsByTypeB = ImmutableSetMultimap.builder();
        for (Relationship parentRelationship : parentRelationships) {
            Node parentNode = parentRelationship.getEndNode();
            parentsByTypeB.put(parentNode.getProperty(_TYPE, "").toString(), parentNode);
        }
        ImmutableSetMultimap<String, Node> parentsByType = parentsByTypeB.build();
        if (parentsByType.size() == 0) {
            return ImmutableSet.of(nodesByType);
        } else if (!lineages.parentsCardinality) {
            Map<String, Node> result = parentsByType.asMap().values().stream()
                    .map(parents -> parents.iterator().next())
                    .map(parent -> doRewind(parent, Maps.newHashMap(nodesByType), lineages))
                    .map(lineage -> {
                        if (lineage.size() > 1)
                            throw new IllegalArgumentException("Can't have more than 1 Lineage while parentsCardinality = false");
                        if (lineage.size() < 1)
                            throw new IllegalArgumentException("Can't have no lineage");
                        return lineage.stream().findFirst().get();
                    }).reduce(new HashMap<>(), (m1, m2) -> {
                        m1.putAll(m2);
                        return m1;
                    });
            return ImmutableSet.of(result);
        } else {
            return parentsByType.asMap().values().stream()
                    .map(parents -> parents.stream()
                            .map(parent -> doRewind(parent, Maps.newHashMap(nodesByType), lineages))
                            .reduce(ImmutableSet.of(), Sets::union)
                    ).reduce(ImmutableSet.of(), this::mergeBranches);
        }
    }

    private Set<Map<String, Node>> mergeBranches(Set<Map<String, Node>> left, Set<Map<String, Node>> right) {
        if (left.isEmpty())
            return right;
        if (right.isEmpty())
            return left;
        return Sets.cartesianProduct(left, right).stream()
                .map(list -> {
                    HashMap<String, Node> builder = new HashMap<>();
                    list.forEach(builder::putAll);
                    return ImmutableMap.copyOf(builder);
                }).collect(ImmutableSet.toImmutableSet());
    }

    private void writeCSVLine(ExportQueryResult result, CSVWriter csv) {
        try {
            String[] line = result.result.values().stream()
                    .flatMap(properties -> properties.values().stream())
                    .map(PropertyConverter::asString)
                    .toArray(String[]::new);
            csv.writeNext(line, false);
            csv.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the Lineage given as input if it matches the filters, empty otherwise.
     */
    private Optional<Lineage> filterLineage(ExportQuery exportQuery, Lineages lineages, Lineage lineage) {
        int missingRequired = 0;
        boolean filtered = exportQuery.filter
                .test(column -> {
                    Node node = lineage.nodesByType.get(column.keyAttribute);
                    return node != null ? lineage.getProperty(column.keyAttribute, column.property) : null;
                });
        if (!filtered) {
            return Optional.empty();
        }
        for (String attribute : lineages.attributesToExport) {
            Node node = lineage.nodesByType.get(attribute);
            if (node == null) {
                if (exportQuery.requiredAttributes.contains(attribute)) {
                    missingRequired++;
                    if (missingRequired == exportQuery.requiredAttributes.size()) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.of(lineage);
    }

    private String[] generateCSVHeader(PaginatedLineages lineages) {
        List<String> header = Lists.newArrayList();
        for (String attribute : lineages.attributesToExport()) {
            SortedMap<String, String> properties = lineages.header().get(attribute);
            if (properties != null) {
                String[] split = attribute.split(":");
                String attributeName = split[1];
                for (Map.Entry<String, String> property : properties.entrySet()) {
                    header.add(attributeName + '.' + property.getKey());
                }
            }
        }
        return header.toArray(new String[0]);
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    public interface PaginatedLineages {
        ImmutableSet<String> attributesToExport();

        Map<String, SortedMap<String, String>> header();

        List<Pair<List<ExportQueryResult>, List<RelationshipQueryResult>>> results();

        int start();

        int end();

        int total();

    }

    private static final PaginatedLineages EMPTY_PAGINATED_LINEAGE = new PaginatedLineages() {
        @Override
        public ImmutableSet<String> attributesToExport() {
            return ImmutableSet.of();
        }

        @Override
        public Map<String, SortedMap<String, String>> header() {
            return ImmutableMap.of();
        }

        @Override
        public List<Pair<List<ExportQueryResult>, List<RelationshipQueryResult>>> results() {
            return ImmutableList.of();
        }

        @Override
        public int start() {
            return 0;
        }

        @Override
        public int end() {
            return 0;
        }

        @Override
        public int total() {
            return 0;
        }
    };
}
