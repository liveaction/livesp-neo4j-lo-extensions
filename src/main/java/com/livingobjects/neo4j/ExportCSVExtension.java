package com.livingobjects.neo4j;


import au.com.bytecode.opencsv.CSVWriter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.HeaderElement;
import com.livingobjects.neo4j.iwan.model.IwanModelConstants;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.TAG;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants._TYPE;

@Path("/export")
public final class ExportCSVExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadCSVExtension.class);
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

    private void runLineage(Node currentNode, ImmutableList<String> lineageAttributes, Lineage lineage, Lineages lineages) throws LineageCardinalityException {
        String tag = currentNode.getProperty(TAG).toString();
        String type = currentNode.getProperty(_TYPE).toString();
        lineage.nodesByType.put(type, currentNode);
        lineages.markAsVisited(tag, type, currentNode);
        Iterable<Relationship> parentRelationships = currentNode.getRelationships(Direction.OUTGOING, IwanModelConstants.LINK_CONNECT);
        for (Relationship parentRelationship : parentRelationships) {
            Node parentNode = parentRelationship.getEndNode();
            String parentType = parentNode.getProperty(_TYPE, "").toString();
            if (lineageAttributes.contains(parentType)) {
                String parentTag = parentNode.getProperty(TAG).toString();
                Node existingNode = lineage.nodesByType.get(parentType);
                if (existingNode == null) {
                    runLineage(parentNode, difference(lineageAttributes, parentType), lineage, lineages);
                } else {
                    String existingTag = existingNode.getProperty(TAG).toString();
                    if (!existingTag.equals(parentTag)) {
                        throw new LineageCardinalityException(lineage, existingTag, parentTag);
                    }
                }
            }
        }
    }

    private long export(Request request, OutputStream outputStream) {

        ImmutableList<String> attributesToExport = ImmutableList.copyOf(request.attributesToExport);
        try (Transaction ignored = graphDb.beginTx()) {
            Lineages lineages = new Lineages();
            if (!attributesToExport.isEmpty()) {
                for (int index = attributesToExport.size() - 1; index > 0; index--) {
                    String leafAttribute = attributesToExport.get(index);
                    ImmutableList<String> lineageAttributes = attributesToExport.subList(0, index);
                    ResourceIterator<Node> leaves = graphDb.findNodes(IwanModelConstants.LABEL_NETWORK_ELEMENT, IwanModelConstants._TYPE, leafAttribute);
                    while (leaves.hasNext()) {
                        Node leaf = leaves.next();
                        if (!lineages.dejaVu(leaf)) {
                            try {
                                Lineage lineage = new Lineage();
                                runLineage(leaf, lineageAttributes, lineage, lineages);
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
                List<String> header = Lists.newArrayList();
                for (String attribute : attributesToExport) {
                    Map<String, String> properties = lineages.propertiesTypeByType.get(attribute);
                    for (Map.Entry<String, String> property : properties.entrySet()) {
                        header.add(attribute + '.' + property.getKey() + ':' + property.getValue());
                    }
                }
                csvWriter.writeNext(header.toArray(new String[header.size()]));
                for (Lineage lineage : lineages.lineages) {
                    String[] line = new String[header.size()];
                    int index = 0;
                    for (String attribute : attributesToExport) {
                        Node node = lineage.nodesByType.get(attribute);
                        Map<String, String> properties = lineages.propertiesTypeByType.get(attribute);
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
                            }
                            index++;
                        }
                    }
                    csvWriter.writeNext(line);
                    lines++;
                }
                return lines;
            }

        } catch (Exception e) {
            LOGGER.error("export-csv extension : ", e);
            throw new RuntimeException(e);
        }
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

        public Request(
                @JsonProperty("attributesToExport") List<String> attributesToExport,
                @JsonProperty("requiredAttributes") List<String> requiredAttributes) {
            this.attributesToExport = attributesToExport;
            this.requiredAttributes = requiredAttributes;
        }
    }

    private ImmutableList<String> difference(ImmutableList<String> attributes, String attributeToRemove) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        attributes.stream()
                .filter(attribute -> !attribute.equals(attributeToRemove))
                .forEach(builder::add);
        return builder.build();
    }

    private static final class Lineages {

        private static final Set<String> INGNORE = ImmutableSet.of("createdAt", "updatedAt", "createdBy", "updatedBy");

        public final List<Lineage> lineages;

        public final Set<String> allTags;

        public final Map<String, Map<String, String>> propertiesTypeByType;

        public Lineages() {
            lineages = Lists.newArrayList();
            allTags = Sets.newHashSet();
            propertiesTypeByType = Maps.newHashMap();
        }

        public boolean dejaVu(Node leaf) {
            return allTags.contains(leaf.getProperty(TAG).toString());
        }

        public void markAsVisited(String nodeTag, String type, Node node) {
            allTags.add(nodeTag);
            Map<String, String> properties = propertiesTypeByType.computeIfAbsent(type, k -> Maps.newHashMap());
            for (Map.Entry<String, Object> property : node.getAllProperties().entrySet()) {
                String name = property.getKey();
                String propertyType = getPropertyType(property.getValue());
                if (!name.startsWith("_") && !INGNORE.contains(name)) {
                    properties.put(name, propertyType);
                }
            }
        }

        public void add(Lineage lineage) {
            lineages.add(lineage);
        }

        public static String getPropertyType(Object value) {
            Class<?> clazz = value.getClass();
            if (clazz.isArray()) {
                return getSimpleType(clazz.getComponentType()) + "[]";
            } else {
                return getSimpleType(clazz);
            }
        }

        public static String getSimpleType(Class<?> clazz) {
            if (clazz.isAssignableFrom(Number.class)) {
                return HeaderElement.Type.NUMBER.name();
            } else if (clazz.isAssignableFrom(Boolean.class)) {
                return HeaderElement.Type.BOOLEAN.name();
            } else {
                return HeaderElement.Type.STRING.name();
            }
        }

    }

    static final class Lineage {
        public final Map<String, Node> nodesByType;

        public Lineage() {
            this.nodesByType = Maps.newHashMap();
        }

        @Override
        public String toString() {
            return nodesByType.entrySet().stream().map(e -> e.getValue().getProperty(TAG).toString()).collect(Collectors.joining(" - "));
        }
    }

}
