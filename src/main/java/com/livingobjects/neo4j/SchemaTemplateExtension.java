package com.livingobjects.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.livingobjects.neo4j.loader.SchemaTemplateLoader;
import com.livingobjects.neo4j.model.MemdexPath;
import com.livingobjects.neo4j.model.exception.SchemaTemplateException;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jErrorResult;
import com.sun.jersey.multipart.MultiPart;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

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
                String json = JSON_MAPPER.writeValueAsString(appliedTemplate);
                return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
            }
        } catch (Throwable e) {
            LOGGER.error("Unable to update schemas", e);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{id}")
    @Produces({"application/json", "text/plain"})
    public Response getSchema(@PathParam("id") String schemaId) throws IOException {
        Transaction tx = graphDb.beginTx();
        Node schemaNode = graphDb.findNode(Labels.SCHEMA, ID, schemaId);
        if (schemaNode == null) {
            return errorResponse(new NoSuchElementException("Schema " + schemaId + " not found in database !"));
        }

        StreamingOutput stream = outputStream -> {
            JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);


            jg.writeStartObject();
            jg.writeStringField(ID, schemaId);
            jg.writeStringField(VERSION, schemaNode.getProperty(VERSION, "0").toString());
            jg.flush();

            jg.writeObjectFieldStart("realms");
            schemaNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PROVIDED).forEach(rel -> {
                try {
                    Node realmTemplateNode = rel.getEndNode();
                    Iterable<Relationship> planetRels = realmTemplateNode.getRelationships(Direction.OUTGOING, RelationshipTypes.MEMDEXPATH);
                    Node firstSegment = Iterables.getOnlyElement(planetRels).getEndNode();
                    MemdexPath memdexPath = browsePlanetToMemdexPath(firstSegment);
                    jg.writeObjectField(realmTemplateNode.getProperty(TEMPLATE).toString(), memdexPath);
                    jg.flush();
                } catch (IOException e) {
                    LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("STACKTRACE", e);
                    }
                }
            });
            tx.close();

            jg.writeEndObject();
            jg.writeEndObject();
            jg.flush();
            jg.close();
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

    }

    private MemdexPath browsePlanetToMemdexPath(Node planet) {

        ImmutableList.Builder<String> attributes = ImmutableList.builder();
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

        ImmutableList.Builder<Map<String, Object>> counters = ImmutableList.builder();
        planet.getRelationships(RelationshipTypes.PROVIDED, Direction.INCOMING).forEach(link -> {
            Node counterNode = link.getStartNode();
            ImmutableMap<String, Object> properties = ImmutableMap.copyOf(counterNode.getAllProperties());
            counters.add(properties);
        });

        ImmutableList.Builder<MemdexPath> memdexpaths = ImmutableList.builder();
        planet.getRelationships(RelationshipTypes.MEMDEXPATH, Direction.OUTGOING).forEach(link -> {
            Node nextPlanetNode = link.getEndNode();
            memdexpaths.add(browsePlanetToMemdexPath(nextPlanetNode));
        });

        return MemdexPath.build(
                planet.getProperty(PATH).toString(),
                planet.getProperty(TEMPLATE).toString(),
                attributes.build(),
                counters.build(),
                memdexpaths.build());
    }

    private Response errorResponse(Throwable cause) throws IOException {
        String code = cause.getClass().getName();
        Neo4jErrorResult error = new Neo4jErrorResult(code, cause.getMessage());
        String json = JSON_MAPPER.writeValueAsString(error);
        return Response.serverError().entity(json).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
