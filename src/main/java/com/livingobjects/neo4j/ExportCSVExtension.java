package com.livingobjects.neo4j;


import au.com.bytecode.opencsv.CSVWriter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    private void getLineage(Node node, ImmutableList<String> attributesToExport, List<Node> currentLineage, List<List<Node>> globalLineage) {
        int relationShips = 0;
        Iterable<Relationship> relationships = node.getRelationships(Direction.INCOMING, IwanModelConstants.LINK_CONNECT);
        for (Relationship relationship : relationships) {
            Node incomingNode = relationship.getStartNode();
            String nodeType = incomingNode.getProperty(IwanModelConstants._TYPE, "").toString();
            if (attributesToExport.contains(nodeType)) {
                relationShips++;
                ImmutableList<Node> copy = ImmutableList.<Node>builder().addAll(currentLineage).add(incomingNode).build();
                ImmutableList<String> subAttributes = difference(attributesToExport, nodeType);
                getLineage(incomingNode, subAttributes, copy, globalLineage);
            }
        }
        if (relationShips == 0) {
            for (int index = 0; index < globalLineage.size(); index++) {
                List<Node> baseLineage = globalLineage.get(index);
                if (baseLineage.containsAll(currentLineage)) {
                    break;
                } else if (currentLineage.containsAll(baseLineage)) {
                    globalLineage.add(currentLineage);
                    globalLineage.remove(index);
                    break;
                }
            }

        }
    }

    private long export(Request request, OutputStream outputStream) {

        ImmutableList<String> attributesToExport = ImmutableList.copyOf(request.attributesToExport);

        try (Transaction ignored = graphDb.beginTx()) {
            List<List<Node>> allLineages = Lists.newArrayList();
            Iterator<String> iterator = request.attributesToExport.iterator();
            if (iterator.hasNext()) {
                String startAttribute = iterator.next();
                ImmutableList<String> subAttributes = difference(attributesToExport, startAttribute);
                ResourceIterator<Node> nodes = graphDb.findNodes(IwanModelConstants.LABEL_NETWORK_ELEMENT, IwanModelConstants._TYPE, startAttribute);
                while (nodes.hasNext()) {
                    Node node = nodes.next();
                    getLineage(node, subAttributes, ImmutableList.of(node), allLineages);
                }
            }

            try (CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(outputStream))) {
                long lines = 0;
                for (List<Node> lineage : allLineages) {
                    if (lineage != null) {
                        String[] line = lineage.stream().map(node -> node.getProperty(IwanModelConstants.TAG, "").toString()).toArray(String[]::new);
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

    private List<List<Node>> merge(List<List<Node>> allLineages) {
        for (int index = 0; index < allLineages.size(); index++) {
            List<Node> lineage = allLineages.get(index);
            for (int baseIndex = index + 1; baseIndex < allLineages.size(); baseIndex++) {
                List<Node> baseLineage = allLineages.get(baseIndex);
                if (baseLineage != null) {
                    if (baseLineage.containsAll(lineage)) {
                        allLineages.set(index, null);
                    }
                }
            }
        }
        return allLineages;
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
}
