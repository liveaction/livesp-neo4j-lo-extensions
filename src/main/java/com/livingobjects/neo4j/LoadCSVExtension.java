package com.livingobjects.neo4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;
import com.livingobjects.neo4j.loader.CsvTopologyLoader;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import com.livingobjects.neo4j.model.result.Neo4jLoadResult;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

@Path("/load-csv")
public final class LoadCSVExtension {

    private final Log logger;

    private static final MediaType TEXT_CSV_MEDIATYPE = MediaType.valueOf("text/csv");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final GraphDatabaseService graphDb;

    private final MetricRegistry metrics = new MetricRegistry();

    public LoadCSVExtension(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.logger = log;
    }

    @POST
    @Consumes("multipart/mixed")
    public Response loadCSV(MultiPart multiPart) throws IOException {
        Stopwatch sWatch = Stopwatch.createStarted();
        long importedElementsCounter = 0;
        try (Timer.Context ignore = metrics.timer("loadCSV").time()) {
            File csv = multiPart.getBodyParts().stream()
                    .filter(bp -> TEXT_CSV_MEDIATYPE.equals(bp.getMediaType()))
                    .map(bp -> bp.getEntityAs(File.class))
                    .findAny()
                    .orElseThrow(IllegalArgumentException::new);

            try (InputStream is = new FileInputStream(csv)) {
                Neo4jLoadResult result = new CsvTopologyLoader(graphDb, metrics, logger).loadFromStream(is);
                importedElementsCounter = result.importedElementsByScope.values().size();
                String json = JSON_MAPPER.writeValueAsString(result);
                return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();

            } catch (Throwable e) {
                logger.error("load-csv extension : unable to execute query", e);
                if (e.getCause() != null) {
                    return errorResponse(e.getCause());
                } else {
                    return errorResponse(e);
                }
            } finally {
                if (csv != null) {
                    csv.delete();
                }
            }

        } catch (IllegalArgumentException e) {
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();
        } catch (Exception e) {
            return errorResponse(e);

        } finally {
            logger.info("Import %s element(s) in %s ms.", importedElementsCounter, sWatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
