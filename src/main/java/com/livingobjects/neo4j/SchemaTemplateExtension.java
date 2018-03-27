package com.livingobjects.neo4j;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import com.livingobjects.neo4j.model.schema.RealmNode;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanets;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanetsUpdate;
import com.livingobjects.neo4j.schema.SchemaLoader;
import com.livingobjects.neo4j.schema.SchemaReader;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.VERSION;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

@Path("/schema")
public class SchemaTemplateExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTemplateExtension.class);

    private final GraphDatabaseService graphDb;
    private final ObjectMapper json = new ObjectMapper();

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public SchemaTemplateExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    @Produces({"application/json", "text/plain"})
    @Consumes(MediaType.APPLICATION_JSON)
    public Response loadSchema(String jsonBody) throws IOException {
        try (JsonParser jsonParser = json.getJsonFactory().createJsonParser(jsonBody)) {
            SchemaLoader schemaLoader = new SchemaLoader(graphDb);
            SchemaAndPlanets schema = jsonParser.readValueAs(SchemaAndPlanets.class);
            boolean updated = schemaLoader.load(schema);
            return Response.ok().entity('"' + String.valueOf(updated) + '"').type(MediaType.APPLICATION_JSON).build();
        } catch (Throwable e) {
            LOGGER.error("Unable to load schema", e);
            return errorResponse(e);
        }
    }

    @PUT
    @Produces({"application/json", "text/plain"})
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateSchema(String jsonBody) throws IOException {
        try (JsonParser jsonParser = json.getJsonFactory().createJsonParser(jsonBody)) {
            SchemaLoader schemaLoader = new SchemaLoader(graphDb);
            SchemaAndPlanetsUpdate schemaAndPlanetsUpdate = jsonParser.readValueAs(SchemaAndPlanetsUpdate.class);
            boolean updated = schemaLoader.update(schemaAndPlanetsUpdate);
            return Response.ok().entity('"' + String.valueOf(updated) + '"').type(MediaType.APPLICATION_JSON).build();
        } catch (Throwable e) {
            LOGGER.error("Unable to load schema", e);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{id}")
    @Produces({"application/json", "text/plain"})
    public Response getSchema(@PathParam("id") String schemaId) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            Node schemaNode = graphDb.findNode(Labels.SCHEMA, ID, schemaId);

            if (schemaNode == null) {
                return errorResponse(new NoSuchElementException("Schema " + schemaId + " not found in database !"));
            }
        }

        StreamingOutput stream = outputStream -> {
            List<Node> realmNodes = Lists.newArrayList();
            try (JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
                 Transaction tx = graphDb.beginTx()) {
                Node schemaNode = graphDb.findNode(Labels.SCHEMA, ID, schemaId);

                if (schemaNode == null) {
                    throw new NoSuchElementException("Schema " + schemaId + " not found in database !");
                }

                jg.writeStartObject();
                jg.writeStringField(ID, schemaId);
                jg.writeStringField(VERSION, schemaNode.getProperty(VERSION, "0").toString());

                schemaNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PROVIDED).forEach(rel -> {
                    Node targetNode = rel.getEndNode();
                    if (targetNode.hasLabel(Labels.REALM_TEMPLATE)) {
                        realmNodes.add(targetNode);
                    }
                });

                Map<String, Node> countersDictionary = Maps.newHashMap();
                Map<String, RealmNode> realms = Maps.newHashMap();
                realmNodes.forEach(realmNode -> {
                    String name = realmNode.getProperty(NAME).toString();
                    try {
                        Relationship firstMemdexPath = realmNode.getSingleRelationship(RelationshipTypes.MEMDEXPATH, Direction.OUTGOING);
                        if (firstMemdexPath != null) {
                            Node segment = firstMemdexPath.getEndNode();
                            Entry<MemdexPathNode, Map<String, Node>> segments = SchemaReader.browseSegments(segment);
                            countersDictionary.putAll(segments.getValue());
                            List<String> attributes = Lists.newArrayList();
                            Iterable<Relationship> attributesRel = realmNode.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING);
                            for (Relationship attribute : attributesRel) {
                                String attType = attribute.getEndNode().getProperty(_TYPE).toString();
                                String attName = attribute.getEndNode().getProperty(NAME).toString();
                                attributes.add(attType + ":" + attName);
                            }
                            RealmNode realm = new RealmNode(name, attributes, segments.getKey());
                            realms.put("realm:" + name, realm);
                        } else {
                            LOGGER.warn("Empty RealmTemplate '{}' : no MdxPath relationship found. Ignoring it", name);
                        }
                    } catch (NotFoundException e) {
                        throw new IllegalStateException(String.format("Malformed RealmTemplate '%s' : more than one root MdxPath relationships found.", name));
                    }
                });

                jg.writeObjectFieldStart("counters");
                jg.flush();
                countersDictionary.forEach((key, value) -> {
                    try {
                        ObjectNode counter = new ObjectNode(JsonNodeFactory.instance);
                        String context = key.split("@context:")[1];
                        counter.put("type", "counter");
                        counter.put("context", context);
                        counter.put("unit", value.getProperty("unit").toString());
                        counter.put("defaultValue", Optional.ofNullable(value.getProperty("defaultValue", null)).map(Object::toString).orElse(null));
                        counter.put("defaultAggregation", value.getProperty("defaultAggregation").toString());
                        counter.put("valueType", value.getProperty("valueType").toString());
                        counter.put("name", value.getProperty("name").toString());
                        jg.writeObjectField(key, counter);
                    } catch (IOException e) {
                        LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("STACKTRACE", e);
                        }
                    }
                });
                jg.writeEndObject();

                jg.writeObjectFieldStart("realms");
                jg.flush();
                realms.forEach((key, value) -> {
                    try {
                        jg.writeObjectField(key, value);
                        jg.flush();
                    } catch (IOException e) {
                        LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("STACKTRACE", e);
                        }
                    }
                });

                jg.writeEndObject();
                jg.writeEndObject();
                jg.flush();
            } catch (Throwable e) {
                LOGGER.error("Unable to load schema '{}'", schemaId, e);
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
