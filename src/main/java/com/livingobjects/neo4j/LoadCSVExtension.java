package com.livingobjects.neo4j;

import com.livingobjects.neo4j.iwan.IWanTopologyLoader;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
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
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

@Path("/load-csv")
public final class LoadCSVExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadCSVExtension.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    private final GraphDatabaseService graphDb;

    public LoadCSVExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    @Consumes("multipart/mixed")
    public Response loadCSV(MultiPart multiPart) throws IOException, ServletException {
        try {
            File csvEntity = null;
            if (multiPart.getBodyParts().size() < 1) {
                csvEntity = multiPart.getEntityAs(File.class);
            } else {
                BodyPart part = multiPart.getBodyParts().get(1);
                csvEntity = part.getEntityAs(File.class);
            }
            try (InputStream is = new FileInputStream(csvEntity)) {
                Neo4jResult result = new IWanTopologyLoader(graphDb).loadFromStream(is);
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
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jError error = new Neo4jError(code, cause.getMessage());
        Map<String, Neo4jError> errorsMap = new HashMap<>();
        errorsMap.put("error", error);
        String json = JSON_MAPPER.writeValueAsString(errorsMap);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
