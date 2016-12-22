package com.livingobjects.neo4j;

import com.livingobjects.neo4j.iwan.model.exception.SchemaTemplateException;
import com.livingobjects.neo4j.iwan.model.schema.SchemaTemplateLoader;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.map.ObjectMapper;
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

@Path("/schema")
public class SchemaTemplateExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTemplateExtension.class);

    private final GraphDatabaseService graphDb;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public SchemaTemplateExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    @Consumes("multipart/mixed")
    public Response applyTemplate(MultiPart multiPart) throws IOException, ServletException {
        try {

            if (multiPart.getBodyParts().size() != 2) {
                throw new SchemaTemplateException("Request must be a multipart request with two body parts : the csv and the xml");
            }

            File csv = multiPart.getBodyParts().get(0).getEntityAs(File.class);
            File xml = multiPart.getBodyParts().get(1).getEntityAs(File.class);

            try (InputStream csvInputStream = new FileInputStream(csv);
                 InputStream xmlInputStream = new FileInputStream(xml)) {
                SchemaTemplateLoader loader = new SchemaTemplateLoader(graphDb);
                int appliedTemplate = loader.loadAndApplyTemplate(csvInputStream, xmlInputStream);
                LOGGER.info("{} topology schemas updated.", appliedTemplate);
                String json = JSON_MAPPER.writeValueAsString(appliedTemplate);
                return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
            }
        } catch (Throwable e) {
            LOGGER.error("Unable to update schemas", e);
            return errorResponse(e);
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
