package com.livingobjects.neo4j;

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
import javax.ws.rs.POST;
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

@javax.ws.rs.Path("/load-csv")
public final class LoadCSVExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadCSVExtension.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    public static final String FILE_TOKEN = "{file}";

    private final GraphDatabaseService graphDb;

    public LoadCSVExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    @Consumes("multipart/mixed")
    public Response loadCSV(MultiPart multiPart) throws IOException, ServletException {
        try {
            checkMultipartBody(multiPart);
            Neo4jQuery cypherQuery = getQueryPart(multiPart);
            File csvFile = getCSVFilePart(multiPart);
            try {
                Neo4jResult result = loadCSV(cypherQuery, csvFile);
                String json = JSON_MAPPER.writeValueAsString(result);
                return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
            } catch (QueryExecutionException | MalformedURLException e) {
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
        }
    }

    public Neo4jResult loadCSV(Neo4jQuery query, File csvFile) throws QueryExecutionException, MalformedURLException {
        URL csvUrl = csvFile.toURI().toURL();
        LOGGER.info("Loading CSV file '{}'", csvUrl);
        String statement = query.statement.replace(FILE_TOKEN, "'" + csvUrl + "'");
        Result result = graphDb.execute(statement, query.parameters);
        QueryStatistics queryStatistics = result.getQueryStatistics();
        LOGGER.info("CSV loaded : {}", queryStatistics);
        return new Neo4jResult(queryStatistics);
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jError error = new Neo4jError(code, cause.getMessage());
        Map<String, Neo4jError> errorsMap = new HashMap<>();
        errorsMap.put("error", error);
        String json = JSON_MAPPER.writeValueAsString(errorsMap);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
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
