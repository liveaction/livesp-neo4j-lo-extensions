package com.livingobjects.neo4j;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import com.livingobjects.neo4j.model.schema.PartialSchema;
import com.livingobjects.neo4j.model.schema.Schema;
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

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.PATH;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.VERSION;
import static com.livingobjects.neo4j.schema.SchemaReader.browseAttributes;

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
            Schema schema = jsonParser.readValueAs(Schema.class);
            boolean updated = schemaLoader.load(schema);
            return Response.ok().entity('"' + String.valueOf(updated) + '"').type(MediaType.APPLICATION_JSON).build();
        } catch (Throwable e) {
            LOGGER.error("Unable to load schema", e);
            return errorResponse(e);
        }
    }

    @POST
    @Path("/{schemaId}/realm/{realmTemplate}")
    @Produces({"application/json", "text/plain"})
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateSchema(@PathParam("schemaId") String schemaId, @PathParam("realmTemplate") String realmTemplate, String jsonBody) throws IOException {
        try (JsonParser jsonParser = json.getJsonFactory().createJsonParser(jsonBody)) {
            SchemaLoader schemaLoader = new SchemaLoader(graphDb);
            PartialSchema partialSchema = jsonParser.readValueAs(PartialSchema.class);
            boolean updated = schemaLoader.updateRealmPath(schemaId, realmTemplate, partialSchema);
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
                jg.writeObjectFieldStart("planets");
                jg.flush();

                schemaNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PROVIDED).forEach(rel -> {
                    try {
                        Node targetNode = rel.getEndNode();
                        if (targetNode.hasLabel(Labels.PLANET_TEMPLATE)) {
                            String name = targetNode.getProperty("name").toString();
                            jg.writeFieldName(name);
                            jg.writeStartObject();
                            jg.writeStringField("type", "template");
                            jg.writeStringField(NAME, name);
                            jg.writeObjectField("attributes", browseAttributes(targetNode));
                            jg.writeEndObject();
                            jg.flush();

                        } else if (targetNode.hasLabel(Labels.REALM_TEMPLATE)) {
                            realmNodes.add(targetNode);
                        }
                    } catch (IOException e) {
                        LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("STACKTRACE", e);
                        }
                    }
                });
                jg.writeEndObject();

                Map<String, Node> countersDictionary = Maps.newHashMap();
                Map<String, MemdexPathNode> memdexPaths = Maps.newHashMap();
                realmNodes.forEach(realmNode -> {
                    String name = realmNode.getProperty(NAME).toString();
                    try {
                        Relationship firstMemdexPath = realmNode.getSingleRelationship(RelationshipTypes.MEMDEXPATH, Direction.OUTGOING);
                        if (firstMemdexPath != null) {
                            Node segment = firstMemdexPath.getEndNode();
                            Entry<MemdexPathNode, Map<String, Node>> segments = SchemaReader.browseSegments(segment);
                            if (segments == null) {
                                LOGGER.warn("The segment '{}' doesn't extend any PlanetTemplate. Realm '{}' is ignored", segment.getProperty(PATH), name);
                                return;
                            }
                            countersDictionary.putAll(segments.getValue());
                            memdexPaths.put("realm:" + name, segments.getKey());
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
                memdexPaths.forEach((key, value) -> {
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
