package com.livingobjects.neo4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.livingobjects.neo4j.iwan.IWanTopologyLoader;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Path("/load-csv")
public final class LoadCSVExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadCSVExtension.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    private final GraphDatabaseService graphDb;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
            .outputTo(LoggerFactory.getLogger("com.livingobjects.neo4j"))
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    public LoadCSVExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    @Consumes("multipart/mixed")
    public Response loadCSV(MultiPart multiPart) throws IOException, ServletException {
        try (Timer.Context ignore = metrics.timer("loadCSV").time()) {
            File csvEntity;
            if (multiPart.getBodyParts().size() < 1) {
                csvEntity = multiPart.getEntityAs(File.class);
            } else {
                BodyPart part = multiPart.getBodyParts().get(1);
                csvEntity = part.getEntityAs(File.class);
            }
            try (InputStream is = new FileInputStream(csvEntity)) {
                Neo4jLoadResult result = new IWanTopologyLoader(graphDb, metrics).loadFromStream(is);
                String json = JSON_MAPPER.writeValueAsString(result);
                return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();

            } catch (Exception e) {
                LOGGER.error("load-csv extension : unable to execute query", e);
                if (e.getCause() != null) {
                    return errorResponse(e.getCause());
                } else {
                    return errorResponse(e);
                }
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("load-csv extension : bad input format", e);
            return errorResponse(e);
        } finally {
            reporter.report();
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        Map<String, Neo4jErrorResult> errorsMap = new HashMap<>();
        errorsMap.put("error", error);
        String json = JSON_MAPPER.writeValueAsString(errorsMap);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
