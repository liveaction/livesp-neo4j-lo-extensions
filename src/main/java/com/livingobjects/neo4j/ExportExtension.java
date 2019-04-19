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
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.Lineage;
import com.livingobjects.neo4j.model.export.Lineages;
import com.livingobjects.neo4j.model.export.PropertyDefinition;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.export.query.Pagination;
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
import org.neo4j.graphdb.ResourceIterator;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

@Path("/export")
public final class ExportExtension {

    private static final MediaType TEXT_CSV_MEDIATYPE = MediaType.valueOf("text/csv");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportExtension.class);

    private final ObjectMapper json = new ObjectMapper();

    private final GraphDatabaseService graphDb;
    private final MetaSchema metaSchema;

    public ExportExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {
            this.metaSchema = new MetaSchema(graphDb);
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
                        Map<String, PropertyDefinition> properties = allProperties.computeIfAbsent(type, k -> Maps.newHashMap());
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

    private PaginatedLineages paginate(Lineages lineages, List<FilteredLineage> filteredLineages, Optional<Pagination> pagination) {
        int end = pagination
                .map(p -> Math.min(p.offset + p.limit, filteredLineages.size()))
                .orElse(filteredLineages.size());
        return new PaginatedLineages() {

            @Override
            public ImmutableSet<String> attributesToExport() {
                return lineages.attributesToExport;
            }

            @Override
            public Map<String, SortedMap<String, String>> header() {
                return lineages.propertiesTypeByType;
            }

            @Override
            public List<FilteredLineage> lineages() {
                return pagination
                        .map(p -> filteredLineages.subList(p.offset, end))
                        .orElse(filteredLineages);
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

    private PaginatedLineages extract(ExportQuery exportQuery) {
        try (Transaction ignored = graphDb.beginTx()) {
            Lineages lineages = exportLineages(exportQuery);
            List<FilteredLineage> filteredLineages = filter(exportQuery, lineages);
            return paginate(lineages, filteredLineages, exportQuery.pagination);
        }
    }

    private void exportAsCsv(PaginatedLineages paginatedLineages, OutputStream outputStream) {
        try (AutoCloseableCsvWriter csv = Csv.write().to(outputStream).autoClose()) {
            String[] headers = generateCSVHeader(paginatedLineages);
            if (headers.length > 0) {
                try (CsvWriter.Line headerLine = csv.line()) {
                    for (String h : headers) {
                        headerLine.append(h);
                    }
                }
            }
            for (FilteredLineage filteredLineage : paginatedLineages.lineages()) {
                writeCSVLine(filteredLineage, csv);
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    private List<FilteredLineage> filter(ExportQuery exportQuery, Lineages lineages) {
        return ImmutableList.copyOf(lineages.lineages.stream()
                .map(lineage -> filterLineage(exportQuery, lineages, lineage))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(FilteredLineage::new)
                .collect(Collectors.toList()));
    }

    private void exportAsJson(PaginatedLineages paginatedLineages, OutputStream outputStream) {
        try {
            for (FilteredLineage filteredLineage : paginatedLineages.lineages()) {
                String s = JSON_MAPPER.writeValueAsString(filteredLineage.properties);
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
        Lineages lineages = initLineages(exportQuery);
        if (!lineages.attributesToExport.isEmpty()) {
            for (String leafAttribute : lineages.orderedLeafAttributes) {
                ResourceIterator<Node> leaves = graphDb.findNodes(Labels.ELEMENT, GraphModelConstants._TYPE, leafAttribute);
                while (leaves.hasNext()) {
                    Node leaf = leaves.next();
                    if (!lineages.dejaVu(leaf)) {
                        try {
                            Lineage lineage = new Lineage(graphDb);
                            rewindLineage(leaf, lineage, lineages);
                            lineages.add(lineage);
                        } catch (LineageCardinalityException e) {
                            LOGGER.warn("Unable to export lineage {}!={} in {}", e.existingNode, e.parentNode, e.lineage);
                        }
                    }
                }
            }
        }
        return lineages;
    }

    private Lineages initLineages(ExportQuery exportQuery) {
        return new Lineages(metaSchema, exportQuery);
    }

    private void rewindLineage(Node currentNode, Lineage lineage, Lineages lineages) throws LineageCardinalityException {
        String type = currentNode.getProperty(_TYPE, "").toString();
        if (lineages.attributesToExtract.contains(type)) {
            lineage.nodesByType.put(type, currentNode);
        }
        lineages.markAsVisited(type, currentNode);
        Iterable<Relationship> parentRelationships = currentNode.getRelationships(Direction.OUTGOING, RelationshipTypes.CONNECT);
        for (Relationship parentRelationship : parentRelationships) {
            Node parentNode = parentRelationship.getEndNode();
            String parentType = parentNode.getProperty(_TYPE, "").toString();

            String parentTag = parentNode.getProperty(TAG).toString();
            Node existingNode = lineage.nodesByType.get(parentType);
            if (existingNode == null) {
                rewindLineage(parentNode, lineage, lineages);
            } else {
                String existingTag = existingNode.getProperty(TAG).toString();
                if (!existingTag.equals(parentTag)) {
                    throw new LineageCardinalityException(lineage, existingTag, parentTag);
                }
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

    private Optional<ImmutableMap<String, Map<String, Object>>> filterLineage(ExportQuery exportQuery, Lineages lineages, Lineage lineage) {
        int missingRequired = 0;
        boolean filtered = exportQuery.filter
                .test(column -> {
                    Node node = lineage.nodesByType.get(column.keyAttribute);
                    return node != null ? node.getProperty(column.property, null) : null;
                });
        if (!filtered) {
            return Optional.empty();
        }
        Map<String, Map<String, Object>> map = Maps.newLinkedHashMap();
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
            map.put(attribute, getProperties(lineages, lineage, node, attribute));
        }
        return Optional.of(ImmutableMap.copyOf(map));
    }

    private Map<String, Object> getProperties(Lineages lineages, Lineage lineage, Node node, String keyAttribute) {
        Map<String, Object> values = Maps.newLinkedHashMap();
        SortedMap<String, String> properties = lineages.propertiesTypeByType.get(keyAttribute);
        if (node == null) {
            if (properties != null) {
                properties.keySet().forEach(property -> values.put(property, null));
            }
        } else if (properties != null) {
            for (String property : properties.keySet()) {
                Object propertyValue = lineage.getProperty(node, property);
                values.put(property, propertyValue);
            }
        }
        return values;
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

        List<FilteredLineage> lineages();

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
