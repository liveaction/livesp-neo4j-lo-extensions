package com.livingobjects.neo4j;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
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

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.KEYTYPE_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.LINK_PROP_SPECIALIZER;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.PATH;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.VERSION;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._TYPE;

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
    public Response loadSchema() {
        StreamingOutput stream = outputStream -> {
            try (JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
                 Transaction tx = graphDb.beginTx()) {
                jg.writeBoolean(true);
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
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
                jg.writeArrayFieldStart("planets");
                jg.flush();

                schemaNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PROVIDED).forEach(rel -> {
                    try {
                        Node targetNode = rel.getEndNode();
                        if (targetNode.hasLabel(Labels.PLANET_TEMPLATE)) {
                            String name = targetNode.getProperty("name").toString();
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
                jg.writeEndArray();

                Map<String, Node> countersDictionary = Maps.newHashMap();
                Map<String, ObjectNode> memdexPaths = Maps.newHashMap();
                realmNodes.forEach(realmNode -> {
                    String name = realmNode.getProperty(NAME).toString();
                    try {
                        Relationship firstMemdexPath = realmNode.getSingleRelationship(RelationshipTypes.MEMDEXPATH, Direction.OUTGOING);
                        if (firstMemdexPath != null) {
                            Node segment = firstMemdexPath.getEndNode();
                            Entry<ObjectNode, Map<String, Node>> segments = browseSegments(segment);
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
                        value.getAllProperties().forEach((k, v) -> counter.put(k, v.toString()));
                        String context = key.split("@")[1];
                        counter.put("context", context);
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

    @SuppressWarnings("Duplicates")
    private ArrayNode browseAttributes(Node planet) {
        ArrayNode attributes = new ArrayNode(JsonNodeFactory.instance);
        planet.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING).forEach(link -> {
            Node attributeNode = link.getEndNode();
            Object specializer = link.getProperty(LINK_PROP_SPECIALIZER, null);
            Map<String, Object> properties = attributeNode.getProperties(_TYPE, NAME);
            String attribute = properties.get(_TYPE).toString() + KEYTYPE_SEPARATOR + properties.get(NAME).toString();
            if (specializer != null) {
                attribute = attribute + KEYTYPE_SEPARATOR + specializer.toString();
            }
            attributes.add(attribute);
        });

        return attributes;
    }

    /**
     * @param segment The first segment
     * @return An Entry of MemdexPath , List<Counter>
     */
    private Entry<ObjectNode, Map<String, Node>> browseSegments(Node segment) {
        ObjectNode memdexPath = new ObjectNode(JsonNodeFactory.instance);
        Map<String, Node> countersDictionary = Maps.newHashMap();

        ArrayNode planetsNode = new ArrayNode(JsonNodeFactory.instance);
        memdexPath.put("planets", planetsNode);
        segment.getRelationships(RelationshipTypes.EXTEND, Direction.OUTGOING).forEach(extendRel -> {
            Node planetTemplateNode = extendRel.getEndNode();
            String name = planetTemplateNode.getProperty(NAME).toString();
            planetsNode.add("template:" + name);
        });
        if (planetsNode.size() == 0) {
            return null;
        }

        String segmentName = segment.getProperty("path").toString();
        memdexPath.put("segment", segmentName);

        ArrayNode counters = memdexPath.putArray("counters");
        segment.getRelationships(RelationshipTypes.PROVIDED, Direction.INCOMING).forEach(link -> {
            Node counterNode = link.getStartNode();
            if (!counterNode.hasProperty("name") || !link.hasProperty("context")) return;

            String counterRef = "kpi:" + counterNode.getProperty("name") + '@' + link.getProperty("context");
            counters.add(counterRef);
            countersDictionary.putIfAbsent(counterRef, counterNode);
        });

        memdexPath.put("attributes", browseAttributes(segment));

        ArrayNode children = memdexPath.putArray("children");
        segment.getRelationships(RelationshipTypes.MEMDEXPATH, Direction.OUTGOING).forEach(path -> {
            Entry<ObjectNode, Map<String, Node>> entry = browseSegments(path.getEndNode());
            if (entry == null) return;
            children.add(entry.getKey());
            countersDictionary.putAll(entry.getValue());
        });

        return Maps.immutableEntry(memdexPath, countersDictionary);
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
