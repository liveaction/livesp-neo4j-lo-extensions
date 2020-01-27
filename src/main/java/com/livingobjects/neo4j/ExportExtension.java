package com.livingobjects.neo4j;

import com.davfx.ninio.csv.AutoCloseableCsvWriter;
import com.davfx.ninio.csv.Csv;
import com.davfx.ninio.csv.CsvWriter;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.PlanetByContext;
import com.livingobjects.neo4j.helper.PlanetFactory;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.Lineage;
import com.livingobjects.neo4j.model.export.Lineages;
import com.livingobjects.neo4j.model.export.PropertyDefinition;
import com.livingobjects.neo4j.model.export.PropertyNameComparator;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.export.query.Pagination;
import com.livingobjects.neo4j.model.export.query.filter.ValueFilter;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
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
            ExportQuery exportQuery = json.readValue(in, new TypeReference<ExportQuery>() {
            });
            boolean csv = checkAcceptHeader(accept);

            PaginatedLineages lineages = extract(exportQuery);
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

    private PaginatedLineages paginate(Lineages lineages, List<Lineage> filteredLineages, Optional<Pagination> pagination) {
        int end = pagination
                .map(p -> Math.min(p.offset + p.limit, filteredLineages.size()))
                .orElse(filteredLineages.size());
        return new PaginatedLineages() {

            private List<Lineage> lineages() {
                return pagination
                        .map(p -> filteredLineages.subList(p.offset, end))
                        .orElse(filteredLineages);
            }

            @Override
            public void init() {
                lineages().forEach(lineage -> initializePropertiesList(lineages, lineage));
                lineages().forEach(lineage -> initializePropertiesToExport(lineages, lineage));
            }

            @Override
            public ImmutableSet<String> attributesToExport() {
                return lineages.attributesToExport;
            }

            @Override
            public Map<String, SortedMap<String, String>> header() {
                return lineages.propertiesTypeByType;
            }

            @Override
            public List<FilteredLineage> filteredLineages() {
                return lineages()
                        .stream()
                        .map(lineage -> new FilteredLineage(ImmutableMap.copyOf(lineage.propertiesToExportByType)))
                        .collect(Collectors.toList());
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
                return filteredLineages.size();
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
            Map<String, Object> values = lineage.propertiesToExportByType.computeIfAbsent(keyAttribute, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
            List<String> propertiesToExport = Lists.newArrayList(lineages.propertiesTypeByType.get(keyAttribute).keySet());
            for (String property : propertiesToExport) {
                if (!lineages.ignoreProperty(property)) {
                    values.put(property, lineage.getProperty(keyAttribute, property));
                }
            }
        }
    }

    private PaginatedLineages extract(ExportQuery exportQuery) {
        try (Transaction ignored = graphDb.beginTx()) {
            Lineages lineages = exportLineages(exportQuery);
            List<Lineage> filteredLineages = filter(exportQuery, lineages);
            PaginatedLineages paginate = paginate(lineages, filteredLineages, exportQuery.pagination);
            paginate.init();
            return paginate;
        }
    }

    private void exportAsCsv(PaginatedLineages paginatedLineages, OutputStream outputStream) {
        try (AutoCloseableCsvWriter csv = Csv.write().to(outputStream).autoClose()) {
            // If there is no lineage to write, don't even write the headers
            if (paginatedLineages.filteredLineages().size() > 0) {
                String[] headers = generateCSVHeader(paginatedLineages);
                if (headers.length > 0) {
                    try (CsvWriter.Line headerLine = csv.line()) {
                        for (String h : headers) {
                            headerLine.append(h);
                        }
                    }
                }
                for (FilteredLineage lineage : paginatedLineages.filteredLineages()) {
                    writeCSVLine(lineage, csv);
                }
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    private List<Lineage> filter(ExportQuery exportQuery, Lineages lineages) {
        return ImmutableList.copyOf(lineages.lineages().stream()
                .map(lineage -> filterLineage(exportQuery, lineages, lineage))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .sorted(lineages.lineageSortComparator)
                .collect(Collectors.toList()));
    }

    private void exportAsJson(PaginatedLineages paginatedLineages, OutputStream outputStream) {
        try {
            for (FilteredLineage lineage : paginatedLineages.filteredLineages()) {
                String s = JSON_MAPPER.writeValueAsString(lineage.properties);
                outputStream.write(s.getBytes(UTF_8));
                outputStream.write('\n');
            }
        } catch (Throwable e) {
            LOGGER.error("export extension : ", e);
            throw new RuntimeException(e);
        } finally {
            try {
                outputStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    private Lineages exportLineages(ExportQuery exportQuery) {
        ImmutableSet<String> requiredCommonChildren = getCommonChildren(exportQuery.requiredAttributes);
        ImmutableSet<String> filterCommonChildren = Sets.cartesianProduct(exportQuery.requiredAttributes, exportQuery.filter.columns()
                .stream()
                .map(c -> c.keyAttribute)
                .collect(Collectors.toSet()))
                .stream()
                .map(ImmutableSet::copyOf)
                .map(this::getCommonChildren)
                .reduce((set, set2) -> Sets.union(set, set2).immutableCopy())
                .orElseGet(ImmutableSet::of);
        Lineages lineages = initLineages(exportQuery, Sets.union(requiredCommonChildren, filterCommonChildren).immutableCopy());
        if (!lineages.attributesToExport.isEmpty()) {
            for (String leafAttribute : lineages.orderedLeafAttributes) {
                getNodeIterator(leafAttribute, exportQuery).forEach(leaf -> {
                    if (!lineages.dejaVu(leaf)) {
                        try {
                            Lineage lineage = new Lineage(graphDb);
                            rewindLineage(leaf, lineage, lineages);
                            lineages.add(lineage);
                        } catch (LineageCardinalityException e) {
                            LOGGER.warn("Unable to export lineage {}!={} in {}", e.existingNode, e.parentNode, e.lineage);
                        }
                    }
                });
            }
        }
        return lineages;
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
        Set<String> scopes = !exportQuery.scopes.isEmpty() ? exportQuery.scopes :
                exportQuery.filter.columnsFilters().stream()
                        .filter(columnColumnFilter -> metaSchema.isScope(columnColumnFilter.column.keyAttribute))
                        .filter(columnFilter -> columnFilter.column.property.equals(ID))
                        .filter(columnFilter -> columnFilter.valueFilter.operator.equals(ValueFilter.Operator.eq) &&
                                !columnFilter.valueFilter.not)
                        .map(columnFilter -> columnFilter.valueFilter.value.toString())
                        .collect(Collectors.toSet());
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
                        .flatMap(planetNode -> StreamSupport.stream(planetNode.getRelationships(Direction.INCOMING, RelationshipTypes.ATTRIBUTE).spliterator(), false)
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

    private Node getOverridingNode(Node element, List<Node> authorizedPlanets) {
        return StreamSupport.stream(element.getRelationships(INCOMING, EXTEND).spliterator(), false)
                .map(Relationship::getStartNode)
                .filter(node -> authorizedPlanets.contains(Lists.newArrayList(node.getRelationships(OUTGOING, ATTRIBUTE)).get(0).getEndNode()))
                .findAny()
                .map(node -> getOverridingNode(node, authorizedPlanets))
                .orElse(element);
    }

    private Lineages initLineages(ExportQuery exportQuery, Set<String> commonChildren) {
        return new Lineages(metaSchema, exportQuery, commonChildren);
    }

    private void rewindLineage(Node currentNode, Lineage lineage, Lineages lineages) throws LineageCardinalityException {
        String type = currentNode.getProperty(_TYPE, "").toString();
        if (lineages.attributesToExtract.contains(type)) {
            lineage.nodesByType.put(type, currentNode);
        }
        lineages.markAsVisited(currentNode);
        if (lineage.nodesByType.keySet().containsAll(lineages.attributesToExtract)) {
            return;
        }
        Iterable<Relationship> parentRelationships = currentNode.getRelationships(OUTGOING, CONNECT);
        for (Relationship parentRelationship : parentRelationships) {
            Node parentNode = parentRelationship.getEndNode();
            String parentType = parentNode.getProperty(_TYPE, "").toString();

            Node existingNode = lineage.nodesByType.get(parentType);
            if (existingNode == null) {
                rewindLineage(parentNode, lineage, lineages);
            }
        }
    }

    private void writeCSVLine(FilteredLineage filteredLineage, AutoCloseableCsvWriter csv) {
        try (CsvWriter.Line line = csv.line()) {
            filteredLineage.properties.values()
                    .forEach(properties -> properties.values()
                            .forEach(value -> {
                                try {
                                    line.append(PropertyConverter.asString(value));
                                } catch (IOException e) {
                                    throw Throwables.propagate(e);
                                }
                            }));
        } catch (IOException e) {
            throw Throwables.propagate(e);
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

        void init();

        ImmutableSet<String> attributesToExport();

        Map<String, SortedMap<String, String>> header();

        List<FilteredLineage> filteredLineages();

        int start();

        int end();

        int total();

    }

    public static class FilteredLineage {
        public final ImmutableMap<String, Map<String, Object>> properties;

        public FilteredLineage(ImmutableMap<String, Map<String, Object>> properties) {
            this.properties = properties;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilteredLineage that = (FilteredLineage) o;
            return Objects.equals(properties, that.properties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(properties);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("properties", properties)
                    .toString();
        }
    }

}
