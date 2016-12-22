package com.livingobjects.neo4j;

import com.google.common.collect.Iterables;
import com.livingobjects.neo4j.iwan.model.exception.SchemaTemplateException;
import com.livingobjects.neo4j.iwan.model.schema.IWanSchemasLoader;
import com.livingobjects.neo4j.iwan.model.schema.Schema;
import com.livingobjects.neo4j.iwan.model.schema.SchemaResult;
import com.livingobjects.neo4j.iwan.model.schema.SchemaTemplateLoader;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
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
import java.util.List;

@Path("/schema")
public class SchemaTemplateExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerSchemaExtension.class);

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
                SchemaTemplateLoader loader = new SchemaTemplateLoader();
                loader.applyTemplate(csvInputStream, xmlInputStream);
            }

            List<Schema> schemas = JSON_MAPPER.readValue(request, new TypeReference<List<Schema>>() {
            });
            int total = 0;
            Iterable<List<Schema>> partition = Iterables.partition(schemas, 100);
            for (List<Schema> batch : partition) {
                try (Transaction tx = graphDb.beginTx()) {
                    IWanSchemasLoader schemasLoader = new IWanSchemasLoader(graphDb);
                    total += schemasLoader.load(batch);
                    tx.success();
                    LOGGER.debug("Flushing {} schemas...", batch.size());
                }
            }
            LOGGER.info("{} topology schemas updated.", total);
            String json = JSON_MAPPER.writeValueAsString(new SchemaResult(total));
            return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
        } catch (Throwable e) {
            LOGGER.error("Unable to update schemas", e);
            return Response.serverError().entity(e).type(MediaType.APPLICATION_JSON).build();
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
