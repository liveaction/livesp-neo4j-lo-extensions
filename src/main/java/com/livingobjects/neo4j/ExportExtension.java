package com.livingobjects.neo4j;

import com.davfx.ninio.csv.AutoCloseableCsvWriter;
import com.davfx.ninio.csv.Csv;
import com.davfx.ninio.csv.CsvWriter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.Lineage;
import com.livingobjects.neo4j.model.export.Lineages;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import org.codehaus.jackson.annotate.JsonProperty;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Charsets.UTF_8;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
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

    @POST
    public Response export(InputStream in, @HeaderParam("accept") String accept) throws IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();

        AtomicLong lineCounter = new AtomicLong();
        try {

            ExportQuery exportQuery = json.readValue(in, new TypeReference<ExportQuery>() {
            });
            if (TEXT_CSV_MEDIATYPE.equals(MediaType.valueOf(accept))) {
                StreamingOutput stream = outputStream -> lineCounter.set(exportAsCsv(exportQuery, outputStream));
                return Response.ok().entity(stream).type(TEXT_CSV_MEDIATYPE).build();
            } else if (MediaType.APPLICATION_JSON_TYPE.equals(MediaType.valueOf(accept))) {
                StreamingOutput stream = outputStream -> lineCounter.set(exportAsJson(exportQuery, outputStream));
                return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON_TYPE).build();
            } else {
                throw new IllegalArgumentException("'" + accept + "' content type is not supported. Use 'text/csv' or 'application/json'");
            }

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

    private long exportAsCsv(ExportQuery exportQuery, OutputStream outputStream) {
        try (Transaction ignored = graphDb.beginTx()) {
            Lineages lineages = exportLineages(exportQuery);
            try (AutoCloseableCsvWriter csv = Csv.write().to(outputStream).autoClose()) {
                long lines = 0;
                String[] headers = generateCSVHeader(lineages);
                if (headers.length > 0) {
                    try (CsvWriter.Line headerLine = csv.line()) {
                        for (String h : headers) {
                            headerLine.append(h);
                        }
                    }
                }
                for (Lineage lineage : lineages.lineages) {
                    if (writeCSVLine(exportQuery, lineages, lineage, csv)) {
                        lines++;
                    }
                }
                return lines;
            }
        } catch (Exception e) {
            LOGGER.error("export extension : ", e);
            throw new RuntimeException(e);
        }
    }

    private long exportAsJson(ExportQuery exportQuery, OutputStream outputStream) {
        try (Transaction ignored = graphDb.beginTx()) {
            AtomicLong count = new AtomicLong(0);
            Lineages lineages = exportLineages(exportQuery);
            for (Lineage lineage : lineages.lineages) {
                filterLineage(exportQuery, lineages, lineage)
                        .ifPresent(map -> {
                            try {
                                String s = JSON_MAPPER.writeValueAsString(map);
                                outputStream.write(s.getBytes(UTF_8));
                                outputStream.write('\n');
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                            count.incrementAndGet();
                        });
            }
            return count.get();
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
            for (int index = lineages.attributesToExport.size() - 1; index >= 0; index--) {
                String leafAttribute = lineages.attributesToExport.get(index);
                ResourceIterator<Node> leaves = graphDb.findNodes(Labels.ELEMENT, GraphModelConstants._TYPE, leafAttribute);
                while (leaves.hasNext()) {
                    Node leaf = leaves.next();
                    if (!lineages.dejaVu(leaf)) {
                        try {
                            Lineage lineage = new Lineage();
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
        Set<String> attributesToExport = Sets.newTreeSet((o1, o2) -> {
            if (metaSchema.getMonoParentRelations(o1).anyMatch(o2::equals)) {
                return 1;
            } else if (metaSchema.getMonoParentRelations(o2).anyMatch(o1::equals)) {
                return -1;
            } else {
                return o1.compareTo(o2);
            }
        });
        attributesToExport.addAll(exportQuery.parentAttributes);
        attributesToExport.addAll(exportQuery.requiredAttributes);
        return new Lineages(ImmutableList.copyOf(attributesToExport), metaSchema, exportQuery.includeTag);
    }

    private void rewindLineage(Node currentNode, Lineage lineage, Lineages lineages) throws LineageCardinalityException {
        String tag = currentNode.getProperty(TAG).toString();
        String type = currentNode.getProperty(_TYPE, "").toString();
        if (lineages.attributesToExport.contains(type)) {
            lineage.nodesByType.put(type, currentNode);
        }
        lineages.markAsVisited(tag, type, currentNode);
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

    private boolean writeCSVLine(ExportQuery exportQuery, Lineages lineages, Lineage lineage, AutoCloseableCsvWriter csv) {
        return filterLineage(exportQuery, lineages, lineage)
                .map(stringMapMap -> {
                            try (CsvWriter.Line line = csv.line()) {
                                stringMapMap.values()
                                        .forEach(properties -> properties.values().forEach(value -> {
                                            try {
                                                if (value != null) {
                                                    if (value.getClass().isArray()) {
                                                        line.append(json.writeValueAsString(value));
                                                    } else {
                                                        line.append(value.toString());
                                                    }
                                                } else {
                                                    line.append(null);
                                                }
                                            } catch (IOException e) {
                                                throw Throwables.propagate(e);
                                            }
                                        }));
                                return true;
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                ).orElse(false);
    }

    private Optional<Map<String, Map<String, Object>>> filterLineage(ExportQuery exportQuery, Lineages lineages, Lineage lineage) {
        Map<String, Map<String, Object>> map = Maps.newTreeMap(Comparator.comparingInt(lineages.attributesToExport::indexOf));
        for (String attribute : lineages.attributesToExport) {
            Map<String, Object> values = Maps.newLinkedHashMap();
            Node node = lineage.nodesByType.get(attribute);
            SortedMap<String, String> properties = lineages.propertiesTypeByType.get(attribute);
            if (node == null) {
                if (exportQuery.requiredAttributes.contains(attribute)) {
                    return Optional.empty();
                } else if (properties != null) {
                    properties.keySet().forEach(property -> values.put(property, null));
                }
            } else if (properties != null) {
                for (String property : properties.keySet()) {
                    Object propertyValue;
                    if (property.equals(SCOPE)) {
                        propertyValue = getElementScopeFromPlanet(node);
                    } else {
                        propertyValue = node.getProperty(property, null);
                    }
                    values.put(property, propertyValue);
                }
            }
            Map<String, Object> filter = exportQuery.filter.get(attribute);
            if (filter != null) {
                for (Map.Entry<String, Object> filterEntry : filter.entrySet()) {
                    Object value = values.get(filterEntry.getKey());
                    Object filterValue = filterEntry.getValue();
                    if (value == null) {
                        if (filterValue != null) {
                            return Optional.empty();
                        }
                    } else if (!value.equals(filterValue)) {
                        return Optional.empty();
                    }
                }
            }
            map.put(attribute, values);
        }
        return Optional.of(map);
    }

    private String getElementScopeFromPlanet(Node node) {
        Relationship planetRelationship = node.getSingleRelationship(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING);
        if (planetRelationship == null) {
            String tag = node.getProperty(TAG, "").toString();
            throw new IllegalArgumentException(String.format("%s %s=%s is not linked to a planet", Labels.NETWORK_ELEMENT, TAG, tag));
        }
        String scopeTag = planetRelationship.getEndNode().getProperty(SCOPE).toString();
        switch (scopeTag) {
            case SCOPE_GLOBAL_TAG:
                return GLOBAL_SCOPE.id;
            case SCOPE_SP_TAG:
                return SP_SCOPE.id;
            default:
                Node scopeNode = graphDb.findNode(Labels.NETWORK_ELEMENT, TAG, scopeTag);
                if (scopeNode != null) {
                    String[] split = scopeNode.getProperty(_TYPE, ":").toString().split(":");
                    return split[1];
                } else {
                    throw new IllegalArgumentException(String.format("Scope %s=%s cannot be found", TAG, scopeTag));
                }
        }
    }

    private String[] generateCSVHeader(Lineages lineages) {
        List<String> header = Lists.newArrayList();
        for (String attribute : lineages.attributesToExport) {
            SortedMap<String, String> properties = lineages.propertiesTypeByType.get(attribute);
            if (properties != null) {
                for (Map.Entry<String, String> property : properties.entrySet()) {
                    header.add(attribute + '.' + property.getKey() + ':' + property.getValue());
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

    private static final class ExportQuery {

        public final ImmutableSet<String> requiredAttributes;
        public final ImmutableSet<String> parentAttributes;
        public final ImmutableMap<String, Set<String>> columns;
        public final ImmutableMap<String, Map<String, Object>> filter;
        public final boolean includeTag;

        public ExportQuery(@JsonProperty("requiredAttributes") List<String> requiredAttributes,
                           @JsonProperty("parentAttributes") List<String> parentAttributes,
                           @JsonProperty("columns") Map<String, Set<String>> columns,
                           @JsonProperty("filter") Map<String, Map<String, Object>> filter,
                           @JsonProperty("includeTag") boolean includeTag) {
            this.requiredAttributes = ImmutableSet.copyOf(requiredAttributes);
            this.parentAttributes = ImmutableSet.copyOf(parentAttributes);
            this.columns = ImmutableMap.copyOf(columns);
            this.filter = ImmutableMap.copyOf(filter);
            this.includeTag = includeTag;
        }

    }

}
