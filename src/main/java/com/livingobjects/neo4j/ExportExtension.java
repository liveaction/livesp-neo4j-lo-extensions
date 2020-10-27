package com.livingobjects.neo4j;

import com.davfx.ninio.csv.AutoCloseableCsvWriter;
import com.davfx.ninio.csv.Csv;
import com.davfx.ninio.csv.CsvWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.export.query.ExportQueryResult;
import com.livingobjects.neo4j.model.export.query.FullQuery;
import com.livingobjects.neo4j.model.export.query.Pagination;
import com.livingobjects.neo4j.model.export.query.Pair;
import com.livingobjects.neo4j.model.export.query.RelationshipQuery;
import com.livingobjects.neo4j.model.export.query.RelationshipQueryResult;
import com.livingobjects.neo4j.model.export.query.filter.ValueFilter;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.CONNECT;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.CROSS_ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.EXTEND;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

@Path("/export")
public final class ExportExtension {

    private static final MediaType TEXT_CSV_MEDIATYPE = MediaType.valueOf("text/csv");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportExtension.class);

    private final ObjectMapper json = new ObjectMapper();

    private final GraphDatabaseService graphDb;
    private final MetaSchema metaSchema;
    private final TemplatedPlanetFactory templatedPlanetFactory;
    private final PlanetFactory planetFactory;

    public ExportExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {
            this.metaSchema = new MetaSchema(graphDb);
            this.templatedPlanetFactory = new TemplatedPlanetFactory(graphDb);
            this.planetFactory = new PlanetFactory(graphDb);
        }
    }

    @GET
    @Path("/properties")
    public Response exportProperties() throws IOException {
        LOGGER.info("test");
        try (Transaction ignored = graphDb.beginTx()) {
            Map<String, Map<String, PropertyDefinition>> allProperties = Maps.newHashMap();
            graphDb.findNodes(Labels.ELEMENT)
                    .forEachRemaining(node -> {
                        String type = node.getProperty(_TYPE).toString();
                        Map<String, PropertyDefinition> properties = allProperties.computeIfAbsent(type, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
                        node.getAllProperties()
                                .forEach((name, value) -> {
                                    if (!name.startsWith("_") && !properties.containsKey(name)) {
                                        boolean required = isRequired(type, name);
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
    public Response export(InputStream in, @HeaderParam("accept") String accept) throws IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();

        try {
            FullQuery query = json.readValue(in, new TypeReference<FullQuery>() {
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

    private boolean isRequired(String type, String s) {
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

    private PaginatedLineages paginate(List<Lineages> lineages, List<Pair<List<Lineage>, List<Map<String, Object>>>> sortedLines, Optional<Pagination> pagination) {
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
                                        Lists.transform(lineagesAndRelProps.first, l -> l == null ? new ExportQueryResult(ImmutableMap.of()) : new ExportQueryResult(ImmutableMap.copyOf(l.propertiesToExportByType))),
                                        Lists.transform(lineagesAndRelProps.second, props -> props == null ? new RelationshipQueryResult(ImmutableMap.of()) : new RelationshipQueryResult(ImmutableMap.copyOf(props)))
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

    private PaginatedLineages extract(FullQuery fullQuery) {
        try (Transaction ignored = graphDb.beginTx()) {
            List<CrossRelationship> relations = fullQuery.relationshipQueries.stream()
                    .map(rq -> metaSchema.getRelationshipOfType(rq.type))
                    .collect(Collectors.toList());
            List<Lineages> lineages = exportLineages(fullQuery, relations);
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
                                                LOGGER.warn(String.format("Requested property %s could not be found on relation %s, empty value will be returned instead", property, r));
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
     * Given allLineages, recursively fills lines. Each List inside will contain one and only one lineage from each List inside allLineages, with two consecutive lineages being linked
     * by a relationship described in relationQueries and metaRelations.
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
                    List<Pair<Lineage, Relationship>> toAdd = getMatchingLineageAndRelationProperties(l, nextLineages, metaRelations.get(i), relationQueries.get(i));
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
        return Streams.stream(originLineage.nodesByType.get(originType).getRelationships(CROSS_ATTRIBUTE, direction))
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
        try (AutoCloseableCsvWriter csv = Csv.write().to(outputStream).autoClose()) {
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
                    try (CsvWriter.Line headerLine = csv.line()) {
                        for (String h : headers) {
                            headerLine.append(h);
                        }
                    }
                }
                paginatedLineages.results()
                        .stream()
                        .map(list -> list.first.get(0))
                        .forEach(filteredLineage -> writeCSVLine(filteredLineage, csv));
            }
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

    private ImmutableList<Lineages> exportLineages(FullQuery fullQuery, List<CrossRelationship> relations) {
        ImmutableList.Builder<Lineages> lineagesBuilder = ImmutableList.builder();
        for (int i = 0; i < fullQuery.exportQueries.size(); i++) {
            ExportQuery exportQuery = fullQuery.exportQueries.get(i);
            ImmutableSet.Builder<String> requiredChildren = ImmutableSet.<String>builder()
                    .addAll(getCommonChildren(exportQuery.requiredAttributes));
            Optional<RelationshipQuery> previousQuery = i == 0 ? Optional.empty() : Optional.of(fullQuery.relationshipQueries.get(i - 1));
            Optional<RelationshipQuery> nextQuery = i == fullQuery.relationshipQueries.size() ? Optional.empty() : Optional.of(fullQuery.relationshipQueries.get(i));

            Function<RelationshipQuery, Pair<RelationshipQuery, CrossRelationship>> queryToPair = q -> new Pair<>(q, relations.stream().filter(r -> r.type.equals(q.type)).collect(MoreCollectors.onlyElement()));
            Optional<Pair<RelationshipQuery, CrossRelationship>> previousRel = previousQuery.map(queryToPair);
            Optional<Pair<RelationshipQuery, CrossRelationship>> nextRel = nextQuery.map(queryToPair);

            previousRel.ifPresent(q -> requiredChildren.add(q.first.direction == INCOMING ? q.second.originType : q.second.destinationType));
            nextRel.ifPresent(q -> requiredChildren.add(q.first.direction == INCOMING ? q.second.destinationType : q.second.originType));

            lineagesBuilder.add(exportLineagesSingleQuery(requiredChildren.build(), exportQuery, previousRel, nextRel));
        }
        return lineagesBuilder.build();
    }

    private Lineages exportLineagesSingleQuery(ImmutableSet<String> requiredCommonChildren,
                                               ExportQuery exportQuery,
                                               Optional<Pair<RelationshipQuery, CrossRelationship>> previousRel,
                                               Optional<Pair<RelationshipQuery, CrossRelationship>> nextRel) {

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
                .map(this::getCommonChildren)
                .reduce((set, set2) -> Sets.union(set, set2).immutableCopy())
                .orElseGet(ImmutableSet::of);
        ImmutableList.Builder<String> relationAttrToExport = ImmutableList.builder();
        previousRel.ifPresent(pair -> relationAttrToExport.add(pair.first.direction == INCOMING ? pair.second.originType : pair.second.destinationType));
        nextRel.ifPresent(pair -> relationAttrToExport.add(pair.first.direction == INCOMING ? pair.second.destinationType : pair.second.originType));
        Lineages lineages = initLineages(exportQuery, Sets.union(requiredCommonChildren, filterCommonChildren).immutableCopy(), relationAttrToExport.build());
        if (!lineages.attributesToExport.isEmpty() || lineages.noResult) {
            for (String leafAttribute : lineages.orderedLeafAttributes) {
                getNodeIterator(leafAttribute, exportQuery)
                        .filter(nodeFilter(leafAttribute, previousRel, nextRel))
                        .forEach(leaf -> {
                            if (!lineages.dejaVu(leaf)) {
                                Lineage lineage = new Lineage(graphDb);
                                if (!lineages.attributesToExtract.isEmpty()) {
                                    rewindLineage(leaf, lineage, lineages);
                                } else {
                                    // no attributesToExport => this is just used for relationships
                                    lineage.nodesByType.put(leaf.getProperty(_TYPE, "").toString(), leaf);
                                }
                                lineages.add(lineage);
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
    private Predicate<Node> nodeFilter(String
                                               nodeType, Optional<Pair<RelationshipQuery, CrossRelationship>> previousRel, Optional<Pair<RelationshipQuery, CrossRelationship>> nextRel) {
        return node -> {
            AtomicBoolean result = new AtomicBoolean(true);
            // check if previousRel is satisfied
            previousRel.ifPresent(pair -> {
                String destType = pair.first.direction == INCOMING ? pair.second.originType : pair.second.destinationType;
                ImmutableList<ImmutableList<String>> upwardPaths = metaSchema.getUpwardPath(nodeType, destType);
                AtomicBoolean relRes = new AtomicBoolean(false);
                for (ImmutableList<String> path : upwardPaths) {
                    Optional<Node> optParent = getNodeFromPath(node, path);
                    optParent.ifPresent(parent -> relRes.set(relRes.get() ||
                            Streams.stream(parent.getRelationships(CROSS_ATTRIBUTE, pair.first.direction == INCOMING ? OUTGOING : INCOMING))
                                    .anyMatch(r -> pair.first.type.equals(r.getProperty(_TYPE).toString()))));
                }
                result.set(result.get() && relRes.get());
            });

            // check if nextRel is satisfied
            nextRel.ifPresent(pair -> {
                String destType = pair.first.direction == INCOMING ? pair.second.destinationType : pair.second.originType;
                ImmutableList<ImmutableList<String>> upwardPaths = metaSchema.getUpwardPath(nodeType, destType);
                AtomicBoolean relRes = new AtomicBoolean(false);
                for (ImmutableList<String> path : upwardPaths) {
                    Optional<Node> optParent = getNodeFromPath(node, path);
                    optParent.ifPresent(parent -> relRes.set(relRes.get() ||
                            Streams.stream(parent.getRelationships(CROSS_ATTRIBUTE, pair.first.direction))
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
                Streams.stream(initialNode.getRelationships(CONNECT, OUTGOING).iterator())
                        .map(Relationship::getEndNode)
                        .filter(node -> node.getProperty(_TYPE).equals(path.get(0)))
                        .findAny()
                        .flatMap(node -> getNodeFromPath(node, path.subList(1, path.size())));
    }


    /**
     * For a set of types, gives their common children of higher rank. If a common child has childs himself, those won't be returned
     */
    private ImmutableSet<String> getCommonChildren(ImmutableSet<String> types) {
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

    private Stream<Node> getNodeIterator(String leafAttribute, ExportQuery exportQuery) {
        Set<String> initScopes = !exportQuery.scopes.isEmpty() ? exportQuery.scopes :
                exportQuery.filter.columnsFilters().stream()
                        .filter(columnColumnFilter -> metaSchema.isScope(columnColumnFilter.column.keyAttribute))
                        .filter(columnFilter -> columnFilter.column.property.equals(ID))
                        .filter(columnFilter -> columnFilter.valueFilter.operator.equals(ValueFilter.Operator.eq) &&
                                !columnFilter.valueFilter.not)
                        .map(columnFilter -> columnFilter.valueFilter.value.toString())
                        .collect(Collectors.toSet());
        ImmutableSet<String> scopes = getApplicableScopes(initScopes);

        if (scopes.isEmpty()) {
            return graphDb.findNodes(Labels.ELEMENT, GraphModelConstants._TYPE, leafAttribute).stream();
        } else {
            if (metaSchema.isOverridable(leafAttribute)) {
                List<Node> authorizedPlanets = scopes.stream()
                        .flatMap(scopeId -> {
                            PlanetByContext planetByContext = templatedPlanetFactory.getPlanetByContext(leafAttribute);
                            List<String> possibleScopes;
                            if (scopeId.equals(GLOBAL_SCOPE.id)) {
                                possibleScopes = Lists.newArrayList(GLOBAL_SCOPE.id);
                            } else if (scopeId.equals(SP_SCOPE.id)) {
                                possibleScopes = Lists.newArrayList(SP_SCOPE.id, GLOBAL_SCOPE.id);
                            } else {
                                possibleScopes = Lists.newArrayList(scopeId, SP_SCOPE.id, GLOBAL_SCOPE.id);
                            }
                            return possibleScopes.stream()
                                    .flatMap(scope -> planetByContext.allPlanets().stream()
                                            .map(planetTemplate -> planetFactory.get(planetTemplate, scope))
                                            .filter(Objects::nonNull));
                        }).collect(Collectors.toList());
                return authorizedPlanets.stream()
                        .flatMap(planetNode -> StreamSupport.stream(planetNode.getRelationships(INCOMING, RelationshipTypes.ATTRIBUTE).spliterator(), false)
                                .map(Relationship::getStartNode)) // All nodes which do not extend another
                        .map(node -> getOverridingNode(node, authorizedPlanets));
            } else {
                return scopes.stream()
                        .flatMap(scopeId -> {
                            PlanetByContext planetByContext = templatedPlanetFactory.getPlanetByContext(leafAttribute);
                            return planetByContext.allPlanets().stream()
                                    .map(planetTemplate -> planetFactory.get(planetTemplate, scopeId))
                                    .filter(Objects::nonNull)
                                    .flatMap(planetNode -> StreamSupport.stream(planetNode.getRelationships(INCOMING, ATTRIBUTE).spliterator(), false)
                                            .map(Relationship::getStartNode))
                                    .filter(node -> !node.getRelationships(OUTGOING, EXTEND).iterator().hasNext());
                        });
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

    private Node getOverridingNode(Node element, List<Node> authorizedPlanets) {
        return StreamSupport.stream(element.getRelationships(INCOMING, EXTEND).spliterator(), false)
                .map(Relationship::getStartNode)
                .filter(node -> authorizedPlanets.contains(Lists.newArrayList(node.getRelationships(OUTGOING, ATTRIBUTE)).get(0).getEndNode()))
                .findAny()
                .map(node -> getOverridingNode(node, authorizedPlanets))
                .orElse(element);
    }

    private Lineages initLineages(ExportQuery
                                          exportQuery, Set<String> commonChildren, ImmutableList<String> relAttrToExport) {
        return new Lineages(metaSchema, exportQuery, commonChildren, relAttrToExport);
    }

    private void rewindLineage(Node currentNode, Lineage lineage, Lineages lineages) {
        String type = currentNode.getProperty(_TYPE, "").toString();
        if (lineages.attributesToExtract.contains(type)) {
            lineage.nodesByType.put(type, currentNode);
        }
        lineages.markAsVisited(currentNode);
        if (lineage.nodesByType.keySet().containsAll(lineages.attributesToExtract)) {
            return;
        }
        Iterable<Relationship> parentRelationships = currentNode.getRelationships(OUTGOING, CONNECT);
        Lineage copyLineage = new Lineage(lineage);
        for (Relationship parentRelationship : parentRelationships) {
            Node parentNode = parentRelationship.getEndNode();
            String parentType = parentNode.getProperty(_TYPE, "").toString();

            Node existingNode = lineage.nodesByType.get(parentType);
            if (existingNode != null && existingNode.getId() != parentNode.getId() && lineages.parentsCardinality) {
                Lineage newLineage = new Lineage(copyLineage);
                rewindLineage(parentNode, newLineage, lineages);
                lineages.add(newLineage);
            } else if (existingNode == null) {
                rewindLineage(parentNode, lineage, lineages);
            }
        }
    }

    private void writeCSVLine(ExportQueryResult result, AutoCloseableCsvWriter csv) {
        try (CsvWriter.Line line = csv.line()) {
            result.result.values()
                    .forEach(properties -> properties.values()
                            .forEach(value -> {
                                try {
                                    line.append(PropertyConverter.asString(value));
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }));
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
}
