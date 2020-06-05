package com.livingobjects.neo4j;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.livingobjects.neo4j.loader.TopologyLoader;
import com.livingobjects.neo4j.model.iwan.Relationship;
import com.livingobjects.neo4j.model.iwan.RelationshipStatus;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

@Path("/load-relationships")
public final class LoadRelationshipsExtension {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String PARAM_UPDATE_ONLY = "updateOnly";
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadRelationshipsExtension.class);

    private final ObjectMapper json = new ObjectMapper();
    private final TopologyLoader topologyLoader;

    public LoadRelationshipsExtension(@Context DatabaseManagementService dbms) {
        this.topologyLoader = new TopologyLoader(dbms.database(dbms.listDatabases().get(0)));
    }

    @POST
    @Consumes("application/json")
    public Response load(String jsonBody, @QueryParam(PARAM_UPDATE_ONLY) String strUpdateOnly) throws IOException {
        try {
            boolean updateOnly = Boolean.parseBoolean(strUpdateOnly);

            List<RelationshipStatus> status = Lists.newArrayList();
            try (JsonParser jsonParser = json.getFactory().createParser(jsonBody)) {
                TypeReference<List<Relationship>> type = new TypeReference<>() {
                };
                List<Relationship> relationships = jsonParser.readValueAs(type);
                load(relationships, status::add, updateOnly);
            }
            String result = JSON_MAPPER.writeValueAsString(status);
            return Response.ok().entity(result)
                    .type(MediaType.APPLICATION_JSON).build();
        } catch (IllegalArgumentException e) {
            LOGGER.error("load-relationships extension : bad request", e);
            String ex = JSON_MAPPER.writeValueAsString(new Neo4jErrorResult(e.getClass().getSimpleName(), e.getLocalizedMessage()));
            return Response.status(Response.Status.BAD_REQUEST).entity(ex).type(MediaType.APPLICATION_JSON_TYPE).build();
        } catch (Throwable e) {
            LOGGER.error("load-relationships extension : unable to execute query", e);
            if (e.getCause() != null) {
                return errorResponse(e.getCause());
            } else {
                return errorResponse(e);
            }
        }
    }

    private void load(Iterable<Relationship> relationships, Consumer<RelationshipStatus> relationshipStatusConsumer, boolean updateOnly) {
        for (List<Relationship> batch : Lists.partition(ImmutableList.copyOf(relationships), 10000)) {
            topologyLoader.loadRelationships(batch, relationshipStatusConsumer, updateOnly);
        }
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
