package com.livingobjects.neo4j;


import com.davfx.ninio.csv.AutoCloseableCsvWriter;
import com.davfx.ninio.csv.Csv;
import com.davfx.ninio.csv.CsvWriter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import org.neo4j.logging.Log;

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
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

@Path("/export")
public final class ExportCSVExtension {

    private final Log logger;

    private static final MediaType TEXT_CSV_MEDIATYPE = MediaType.valueOf("text/csv");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final ObjectMapper json = new ObjectMapper();

    private final GraphDatabaseService graphDb;
    private final MetaSchema metaSchema;

    public ExportCSVExtension(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.logger = log;
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {
            this.metaSchema = new MetaSchema(graphDb);
        }
    }

    @POST
    public Response exportCSV(InputStream in) throws IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();

        Request request = json.readValue(in, new TypeReference<Request>() {
        });

        AtomicLong lineCounter = new AtomicLong();
        try {
            StreamingOutput stream = outputStream -> lineCounter.set(export(request, outputStream));
            return Response.ok().entity(stream).type(TEXT_CSV_MEDIATYPE).build();

        } catch (IllegalArgumentException e) {
            logger.error("export-csv extension : ", e);
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Response.Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();

        } catch (Exception e) {
            logger.error("export-csv extension : ", e);
            if (e.getCause() != null) {
                return errorResponse(e.getCause());
            } else {
                return errorResponse(e);
            }

        } finally {
            logger.info("Export in {} ms.", stopWatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private long export(Request request, OutputStream outputStream) {
        ImmutableList<String> attributesToExport = ImmutableList.copyOf(request.attributesToExport);
        try (Transaction ignored = graphDb.beginTx()) {
            Lineages lineages = new Lineages(attributesToExport, metaSchema, request.exportTags);
            if (!attributesToExport.isEmpty()) {
                for (int index = attributesToExport.size() - 1; index >= 0; index--) {
                    String leafAttribute = attributesToExport.get(index);
                    ImmutableList<String> lineageAttributes = attributesToExport.subList(0, index);
                    ResourceIterator<Node> leaves = graphDb.findNodes(Labels.NETWORK_ELEMENT, GraphModelConstants._TYPE, leafAttribute);
                    while (leaves.hasNext()) {
                        Node leaf = leaves.next();
                        if (!lineages.dejaVu(leaf)) {
                            try {
                                Lineage lineage = new Lineage();
                                rewindLineage(leaf, lineageAttributes, lineage, lineages);
                                lineages.add(lineage);
                            } catch (LineageCardinalityException e) {
                                logger.warn("Unable to export lineage {}!={} in {}", e.existingNode, e.parentNode, e.lineage);
                            }
                        }
                    }
                }
            }

            try (AutoCloseableCsvWriter csv = Csv.write().to(outputStream).autoClose()) {
                long lines = 0;
                try (CsvWriter.Line headerLine = csv.line()) {
                    for (String h : generateCSVHeader(attributesToExport, lineages)) {
                        headerLine.append(h);
                    }
                }
                for (Lineage lineage : lineages.lineages) {
                    Optional<List<String>> values = writeCSVLine(request, lineages, lineage);
                    if (values.isPresent()) {
                        try (CsvWriter.Line line = csv.line()) {
                            for (String value : values.get()) {
                                line.append(value);
                            }
                        }
                        lines++;
                    }
                }
                return lines;
            }

        } catch (Exception e) {
            logger.error("export-csv extension : ", e);
            throw new RuntimeException(e);
        }
    }

    private void rewindLineage(Node currentNode, ImmutableList<String> lineageAttributes, Lineage lineage, Lineages lineages) throws LineageCardinalityException {
        String tag = currentNode.getProperty(TAG).toString();
        String type = currentNode.getProperty(_TYPE).toString();
        lineage.nodesByType.put(type, currentNode);
        lineages.markAsVisited(tag, type, currentNode);
        Iterable<Relationship> parentRelationships = currentNode.getRelationships(Direction.OUTGOING, RelationshipTypes.CONNECT);
        for (Relationship parentRelationship : parentRelationships) {
            Node parentNode = parentRelationship.getEndNode();
            String parentType = parentNode.getProperty(_TYPE, "").toString();
            if (lineageAttributes.contains(parentType)) {
                String parentTag = parentNode.getProperty(TAG).toString();
                Node existingNode = lineage.nodesByType.get(parentType);
                if (existingNode == null) {
                    rewindLineage(parentNode, difference(lineageAttributes, parentType), lineage, lineages);
                } else {
                    String existingTag = existingNode.getProperty(TAG).toString();
                    if (!existingTag.equals(parentTag)) {
                        throw new LineageCardinalityException(lineage, existingTag, parentTag);
                    }
                }
            }
        }
    }

    private Optional<List<String>> writeCSVLine(Request request, Lineages lineages, Lineage lineage) throws IOException {
        List<String> values = Lists.newArrayList();
        for (String attribute : request.attributesToExport) {
            Node node = lineage.nodesByType.get(attribute);
            SortedMap<String, String> properties = lineages.propertiesTypeByType.get(attribute);
            if (node == null) {
                if (request.requiredAttributes.contains(attribute)) {
                    return Optional.empty();
                } else if (properties != null) {
                    properties.keySet().forEach(property -> values.add(null));
                }
            } else if (properties != null) {
                for (String property : properties.keySet()) {
                    Object propertyValue;
                    if (property.equals(SCOPE)) {
                        propertyValue = getElementScopeFromPlanet(node);
                    } else {
                        propertyValue = node.getProperty(property, null);
                    }
                    if (propertyValue != null) {
                        if (propertyValue.getClass().isArray()) {
                            values.add(json.writeValueAsString(propertyValue));
                        } else {
                            values.add(propertyValue.toString());
                        }
                    } else {
                        values.add(null);
                    }
                }
            }
        }
        return Optional.of(values);
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

    private String[] generateCSVHeader(ImmutableList<String> attributesToExport, Lineages lineages) {
        List<String> header = Lists.newArrayList();
        for (String attribute : attributesToExport) {
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

    private static final class Request {
        final List<String> attributesToExport;
        final List<String> requiredAttributes;
        boolean exportTags;

        public Request(
                @JsonProperty("attributesToExport") List<String> attributesToExport,
                @JsonProperty("requiredAttributes") List<String> requiredAttributes,
                @JsonProperty("exportTags") boolean exportTags) {
            this.attributesToExport = attributesToExport;
            this.requiredAttributes = requiredAttributes;
            this.exportTags = exportTags;
        }

    }

    private ImmutableList<String> difference(ImmutableList<String> attributes, String attributeToRemove) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        attributes.stream()
                .filter(attribute -> !attribute.equals(attributeToRemove))
                .forEach(builder::add);
        return builder.build();
    }

}
