package com.livingobjects.neo4j;


import au.com.bytecode.opencsv.CSVWriter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.export.Lineage;
import com.livingobjects.neo4j.model.export.Lineages;
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

import javax.servlet.ServletException;
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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._TYPE;

@Path("/export")
public final class ExportCSVExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportCSVExtension.class);
    private static final Logger PACKAGE_LOGGER = LoggerFactory.getLogger("com.livingobjects.neo4j");

    private static final MediaType TEXT_CSV_MEDIATYPE = MediaType.valueOf("text/csv");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final ObjectMapper json = new ObjectMapper();

    private final GraphDatabaseService graphDb;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
            .outputTo(PACKAGE_LOGGER)
            .withLoggingLevel(Slf4jReporter.LoggingLevel.DEBUG)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    public ExportCSVExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        if (PACKAGE_LOGGER.isDebugEnabled()) {
            this.reporter.start(5, TimeUnit.MINUTES);
        }
    }

    @POST
    public Response exportCSV(InputStream in) throws IOException, ServletException {
        Stopwatch stopWatch = Stopwatch.createStarted();

        Request request = json.readValue(in, new TypeReference<Request>() {
        });

        AtomicLong lineCounter = new AtomicLong();
        try {
            StreamingOutput stream = outputStream -> lineCounter.set(export(request, outputStream));
            return Response.ok().entity(stream).type(TEXT_CSV_MEDIATYPE).build();

        } catch (IllegalArgumentException e) {
            LOGGER.error("export-csv extension : ", e);
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Response.Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();

        } catch (Exception e) {
            LOGGER.error("export-csv extension : ", e);
            if (e.getCause() != null) {
                return errorResponse(e.getCause());
            } else {
                return errorResponse(e);
            }

        } finally {
            LOGGER.info("Export in {} ms.", stopWatch.elapsed(TimeUnit.MILLISECONDS));
            if (PACKAGE_LOGGER.isDebugEnabled()) {
                reporter.stop();
                reporter.report();
            }
        }
    }

    private long export(Request request, OutputStream outputStream) {
        ImmutableList<String> attributesToExport = ImmutableList.copyOf(request.attributesToExport);
        try (Transaction ignored = graphDb.beginTx()) {
            Lineages lineages = new Lineages(attributesToExport, request.exportTags);
            if (!attributesToExport.isEmpty()) {
                for (int index = attributesToExport.size() - 1; index > 0; index--) {
                    String leafAttribute = attributesToExport.get(index);
                    ImmutableList<String> lineageAttributes = attributesToExport.subList(0, index);
                    ResourceIterator<Node> leaves = graphDb.findNodes(Labels.NETWORK_ELEMENT, IwanModelConstants._TYPE, leafAttribute);
                    while (leaves.hasNext()) {
                        Node leaf = leaves.next();
                        if (!lineages.dejaVu(leaf)) {
                            try {
                                Lineage lineage = new Lineage();
                                rewindLineage(leaf, lineageAttributes, lineage, lineages);
                                lineages.add(lineage);
                            } catch (LineageCardinalityException e) {
                                LOGGER.warn("Unable to export lineage {}!={} in {}", e.existingNode, e.parentNode, e.lineage);
                            }
                        }
                    }
                }
            }

            try (CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(outputStream))) {
                long lines = 0;
                String[] header = generateCSVHeader(attributesToExport, lineages);
                csvWriter.writeNext(header);
                for (Lineage lineage : lineages.lineages) {
                    String[] line = generateCSVLine(request, lineages, header, lineage);
                    if (line != null) {
                        csvWriter.writeNext(line);
                        lines++;
                    }
                }
                return lines;
            }

        } catch (Exception e) {
            LOGGER.error("export-csv extension : ", e);
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

    private String[] generateCSVLine(Request request, Lineages lineages, String[] header, Lineage lineage) throws IOException {
        String[] line = new String[header.length];
        int index = 0;
        for (String attribute : request.attributesToExport) {
            Node node = lineage.nodesByType.get(attribute);
            SortedMap<String, String> properties = lineages.propertiesTypeByType.get(attribute);
            if (properties != null) {
                for (String property : properties.keySet()) {
                    if (node != null) {
                        Object propertyValue = node.getProperty(property, null);
                        if (propertyValue != null) {
                            if (propertyValue.getClass().isArray()) {
                                line[index] = json.writeValueAsString(propertyValue);
                            } else {
                                line[index] = propertyValue.toString();
                            }
                        }
                    } else {
                        if (request.requiredAttributes.contains(attribute)) {
                            return null;
                        }
                    }
                    index++;
                }
            }
        }
        return line;
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
        return header.toArray(new String[header.size()]);
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    private static final class Request {
        public final List<String> attributesToExport;
        public final List<String> requiredAttributes;
        public boolean exportTags;

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
