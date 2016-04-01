package com.livingobjects.neo4j;

import com.livingobjects.neo4j.model.Neo4jError;
import com.livingobjects.neo4j.model.Neo4jQuery;
import com.livingobjects.neo4j.model.Neo4jResult;
import com.livingobjects.neo4j.model.status.Failure;
import com.livingobjects.neo4j.model.status.InProgress;
import com.livingobjects.neo4j.model.status.Status;
import com.livingobjects.neo4j.model.status.Success;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@javax.ws.rs.Path("/load-csv")
public final class LoadCSVExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadCSVExtension.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    public static final String FILE_TOKEN = "{file}";

    private final GraphDatabaseService graphDb;

    private static final ConcurrentHashMap<String, Status> processes = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(20);

    public LoadCSVExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    @Consumes("multipart/mixed")
    public Response loadCSV(MultiPart multiPart, @QueryParam("async") boolean async) throws IOException, ServletException {
        try {
            checkMultipartBody(multiPart);
            Neo4jQuery cypherQuery = getQueryPart(multiPart);
            File csvFile = getCSVFilePart(multiPart);
            if (async) {
                String uuid = UUID.randomUUID().toString();
                processes.put(uuid, new InProgress());
                executor.submit((Runnable) () -> {
                    Status status = executeLoadCSV(cypherQuery, csvFile);
                    processes.put(uuid, status);
                });
                return Response.ok().entity(uuid).type(MediaType.APPLICATION_JSON).build();
            } else {
                Status status = executeLoadCSV(cypherQuery, csvFile);
                return response(status);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("load-csv extension : bad input format", e);
            return errorResponse(asError(e));
        }
    }

    private Response response(Status status) {
        return status.visit(new Status.Visitor<Response>() {

            @Override
            public Response inProgress() {
                return null;
            }

            @Override
            public Response success(Neo4jResult result) {
                try {
                    String json = JSON_MAPPER.writeValueAsString(result);
                    return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
                } catch (IOException e) {
                    return errorResponse(asError(e));
                }
            }

            @Override
            public Response failure(Neo4jError error) {
                return errorResponse(error);
            }
        });
    }

    @GET
    @javax.ws.rs.Path("/{uuid}")
    public Response status(@PathParam("uuid") String uuid) throws IOException, ServletException {
        Status status = processes.get(uuid);
        if (status != null) {
            try {
                String json = JSON_MAPPER.writeValueAsString(status);
                return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
            } catch (IOException e) {
                return errorResponse(asError(e));
            }
        } else {
            return Response.status(Response.Status.NOT_FOUND).entity(uuid).type(MediaType.APPLICATION_JSON).build();
        }
    }

    @DELETE
    @javax.ws.rs.Path("/{uuid}")
    public Response close(@PathParam("uuid") String uuid) throws IOException, ServletException {
        Status status = processes.remove(uuid);
        if (status != null) {
            return Response.ok().entity(status).type(MediaType.APPLICATION_JSON).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).entity(uuid).type(MediaType.APPLICATION_JSON).build();
        }
    }

    private Status executeLoadCSV(Neo4jQuery cypherQuery, File csvFile) {
        try {
            Neo4jResult result = loadCSV(cypherQuery, csvFile);
            return new Success(result);
        } catch (QueryExecutionException | IOException e) {
            LOGGER.error("load-csv extension : unable to execute query", e);
            return new Failure(asError(e));
        }
    }

    private Neo4jResult loadCSV(Neo4jQuery query, File csvFile) throws QueryExecutionException, MalformedURLException {
        URL csvUrl = csvFile.toURI().toURL();
        LOGGER.debug("Loading CSV file '{}'", csvUrl);
        String statement = query.statement.replace(FILE_TOKEN, "'" + csvUrl + "'");
        Result result = graphDb.execute(statement, query.parameters);
        QueryStatistics queryStatistics = result.getQueryStatistics();
        LOGGER.debug("CSV loaded (nodes created : {}, rels created : {}, properties set : {})",
                queryStatistics.getNodesCreated(),
                queryStatistics.getRelationshipsCreated(),
                queryStatistics.getPropertiesSet());
        LOGGER.trace("Detailed query stats : {}", queryStatistics);
        return new Neo4jResult(queryStatistics);
    }

    private Response errorResponse(Neo4jError error) {
        Map<String, Neo4jError> errorsMap = new HashMap<>();
        errorsMap.put("error", error);
        String json;
        try {
            json = JSON_MAPPER.writeValueAsString(errorsMap);
        } catch (IOException e) {
            json = e.getMessage();
        }
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    private Neo4jError asError(Throwable e) {
        Throwable cause = e;
        if (e.getCause() != null) {
            cause = e;
        }
        String code = cause.getClass().getName();
        return new Neo4jError(code, cause.getMessage());
    }

    private File getCSVFilePart(MultiPart multiPart) throws IOException {
        BodyPart part = multiPart.getBodyParts().get(1);
        return part.getEntityAs(File.class);
    }

    private Neo4jQuery getQueryPart(MultiPart multiPart) throws IOException {
        BodyPart bodyPart = multiPart.getBodyParts().get(0);
        try (InputStream in = bodyPart.getEntityAs(InputStream.class)) {
            return JSON_MAPPER.readValue(in, Neo4jQuery.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Request must be of type 'multipart/mixed' with the first body part as 'application/json' being the cypher query :\n{\n\tstatement=\"\",\n\tparameters={...}\n}");
        }
    }

    private void checkMultipartBody(MultiPart multiPart) throws IOException {
        if (multiPart.getBodyParts().size() < 1) {
            throw new IllegalArgumentException("Request must be of type 'multipart/mixed' with two body parts : \nfirst the cypher load statement (application/json)\nand the csv file (text/csv).");
        }
    }

}
