package com.livingobjects.neo4j;

import com.google.common.collect.Iterables;
import com.livingobjects.neo4j.iwan.model.schema.IWanSchemesLoader;
import com.livingobjects.neo4j.iwan.model.schema.Schema;
import com.livingobjects.neo4j.iwan.model.schema.SchemaResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Path("/schema")
public final class CustomerSchemaExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerSchemaExtension.class);

    private final GraphDatabaseService graphDb;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public CustomerSchemaExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    public Response updateSchemes(InputStream request) throws IOException, ServletException {
        try {
            List<Schema> schemes = JSON_MAPPER.readValue(request, new TypeReference<List<Schema>>() {
            });
            int total = 0;
            Iterable<List<Schema>> partition = Iterables.partition(schemes, 100);
            for (List<Schema> batch : partition) {
                try (Transaction tx = graphDb.beginTx()) {
                    IWanSchemesLoader schemesLoader = new IWanSchemesLoader(graphDb);
                    total += schemesLoader.load(batch);
                    tx.success();
                    LOGGER.debug("Flushing {} schemes...", batch.size());
                }
            }
            LOGGER.info("{} topology schemes updated.", total);
            String json = JSON_MAPPER.writeValueAsString(new SchemaResult(total));
            return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
        } catch (Throwable e) {
            LOGGER.error("Unable to update schemes", e);
            return Response.serverError().entity(e).type(MediaType.APPLICATION_JSON).build();
        }
    }

}