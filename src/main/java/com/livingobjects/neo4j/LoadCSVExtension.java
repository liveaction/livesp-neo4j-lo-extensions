package com.livingobjects.neo4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.livingobjects.neo4j.loader.CsvTopologyLoader;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import com.livingobjects.neo4j.model.result.Neo4jLoadResult;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Path("/load-csv")
public final class LoadCSVExtension {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final GraphDatabaseService graphDb;
    private final Log log;

    public LoadCSVExtension(@Context DatabaseManagementService dbms, @Context Log log) {
        this.graphDb = dbms.database(dbms.listDatabases().get(0));
        this.log = log;
    }


    @POST
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    public Response loadCSV(@HeaderParam("X-User") String username, InputStream is) throws IOException {
        Stopwatch sWatch = Stopwatch.createStarted();

        long importedElementsCounter = 0;
        try {
            Neo4jLoadResult result = new CsvTopologyLoader(graphDb, log).loadFromStream(is, username);
            importedElementsCounter = result.importedElementsByScope.values()
                    .stream()
                    .mapToInt(Set::size)
                    .sum();
            String json = JSON_MAPPER.writeValueAsString(result);
            return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
        } catch (IllegalArgumentException e) {
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();
        } catch (Exception e) {
            return errorResponse(e);

        } finally {
            log.info("Import %d element(s) in %d ms.", importedElementsCounter, sWatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
