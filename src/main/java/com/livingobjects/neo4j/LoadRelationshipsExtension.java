package com.livingobjects.neo4j;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.livingobjects.neo4j.loader.TopologyLoader;
import com.livingobjects.neo4j.model.iwan.Relationship;
import com.livingobjects.neo4j.model.iwan.RelationshipStatus;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Path("/load-relationships")
public final class LoadRelationshipsExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadRelationshipsExtension.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String PARAM_UPDATE_ONLY = "updateOnly";

    private final TopologyLoader topologyLoader;

    public LoadRelationshipsExtension(@Context GraphDatabaseService graphDb) {
        this.topologyLoader = new TopologyLoader(graphDb);
    }

    @POST
    @Consumes("application/json")
    public Response load(@Context HttpServletRequest request) throws IOException {
        try {
            Iterable<Relationship> relationships = readRelationships(request.getInputStream());
            boolean updateOnly = Optional.ofNullable(request.getParameter(PARAM_UPDATE_ONLY))
                    .map(Boolean::parseBoolean)
                    .orElse(false);

            StreamingOutput stream = outputStream -> {
                load(relationships, relationshipStatus -> {
                    try {
                        outputStream.write(JSON_MAPPER.writeValueAsBytes(relationshipStatus));
                        outputStream.write("\n".getBytes(Charsets.UTF_8));
                    } catch (IOException e) {
                        LOGGER.error("load-relationships extension : error streaming result", e);
                        throw new IllegalStateException(e);
                    }
                }, updateOnly);
                outputStream.close();
            };
            return Response.ok().entity(stream)
                    .type(MediaType.TEXT_PLAIN_TYPE).build();
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

    private Iterable<Relationship> readRelationships(InputStream inputStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        return () -> new Iterator<Relationship>() {
            String line = null;
            boolean open = true;

            @Override
            public boolean hasNext() {
                try {
                    if (open) {
                        line = reader.readLine();
                        if (line == null) {
                            open = false;
                            reader.close();
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return line != null;
            }

            @Override
            public Relationship next() {
                try {
                    return JSON_MAPPER.readValue(line, Relationship.class);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
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
