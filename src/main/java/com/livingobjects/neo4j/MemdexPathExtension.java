package com.livingobjects.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.iwan.model.MemdexPath;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import javax.servlet.ServletException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.*;

@Path("/memdexpath")
public final class MemdexPathExtension {

    private final GraphDatabaseService graphDb;
    private final ObjectMapper json = new ObjectMapper();

    public MemdexPathExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    public Response memdexPath(InputStream request) throws IOException, ServletException {
        Request filter = json.readValue(request, new TypeReference<Request>() {
        });

        StreamingOutput stream = outputStream -> {
            JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
            jg.writeStartObject();

            try (Transaction ignored = graphDb.beginTx()) {

                ResourceIterator<Node> planets = graphDb.findNodes(LABEL_PLANET);
                while (planets.hasNext()) {
                    Node planetNode = planets.next();
                    Iterable<Relationship> relationships = planetNode.getRelationships(LINK_ATTRIBUTE, Direction.OUTGOING);

                    Optional<String> realm = Optional.empty();
                    Optional<String> dynamicAttribute = Optional.empty();
                    Iterator<Relationship> relationshipIterator = relationships.iterator();
                    int staticMatching = 0;
                    while (relationshipIterator.hasNext() && !(realm.isPresent() && staticMatching == filter.staticAttributes.size() && dynamicAttribute.isPresent())) {
                        Relationship relationship = relationshipIterator.next();

                        Object property = relationship.getEndNode().getProperty(_TYPE, null);
                        if (property != null) {
                            String type = property.toString();
                            String name = relationship.getEndNode().getProperty(NAME).toString();
                            if ("realm".equals(type)) {
                                realm = Optional.of(name);
                            } else {
                                String staticName = filter.staticAttributes.get(type);
                                if (staticName != null) {
                                    if (staticName.equals(name)) {
                                        staticMatching++;
                                    }
                                } else {
                                    String attribute = type + ':' + name;
                                    if (filter.dynamicAttributes.contains(attribute)) {
                                        dynamicAttribute = Optional.of(attribute);
                                    }
                                }
                            }
                        }
                    }

                    if (realm.isPresent() && staticMatching == filter.staticAttributes.size() && dynamicAttribute.isPresent()) {
                        MemdexPath memdexPath = browsePlanetToMemdexPath(planetNode);
                        jg.writeObjectField(dynamicAttribute.get(), new MemdexPathWithRealm(realm.get(), memdexPath));
                    }
                }

            }

            jg.writeEndObject();
            jg.flush();
            jg.close();
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("{realm}")
    public Response memdexPath(@PathParam("realm") final String realm) throws IOException, ServletException {
        StreamingOutput stream = outputStream -> {
            JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
            jg.writeStartArray();

            try (Transaction ignored = graphDb.beginTx()) {
                ResourceIterator<Node> itRealms = graphDb.findNodes(LABEL_ATTRIBUTE, "name", realm);
                Node realmNode = null;
                while (itRealms.hasNext()) {
                    Node tmpNode = itRealms.next();
                    if ("realm".equals(tmpNode.getProperty("_type"))) {
                        realmNode = tmpNode;
                        break;
                    }
                }
                if (realmNode == null) {
                    throw new NoSuchElementException("Element of type 'realm' with name '" + realm + "' not found in database !");
                }

                Node firstPlanet = realmNode.getSingleRelationship(LINK_ATTRIBUTE, Direction.INCOMING).getStartNode();

                MemdexPath memdexPath = browsePlanetToMemdexPath(firstPlanet);

                jg.writeObject(Maps.immutableEntry(realm, memdexPath));
                jg.flush();
            }

            jg.writeEndArray();
            jg.flush();
            jg.close();
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    private MemdexPath browsePlanetToMemdexPath(Node planet) {

        ImmutableList.Builder<String> attributes = ImmutableList.builder();
        planet.getRelationships(LINK_ATTRIBUTE, Direction.OUTGOING).forEach(link -> {
            Node attributeNode = link.getEndNode();
            Map<String, Object> properties = attributeNode.getProperties(_TYPE, NAME);
            attributes.add(properties.get(_TYPE).toString() + KEYTYPE_SEPARATOR + properties.get(NAME).toString());
        });

        ImmutableList.Builder<Map<String, Object>> counters = ImmutableList.builder();
        planet.getRelationships(LINK_PROVIDED, Direction.INCOMING).forEach(link -> {
            Node counterNode = link.getStartNode();
            ImmutableMap<String, Object> properties = ImmutableMap.copyOf(counterNode.getAllProperties());
            counters.add(properties);
        });

        ImmutableList.Builder<MemdexPath> memdexpaths = ImmutableList.builder();
        planet.getRelationships(LINK_MEMDEXPATH, Direction.OUTGOING).forEach(link -> {
            Node nextPlanetNode = link.getEndNode();
            memdexpaths.add(browsePlanetToMemdexPath(nextPlanetNode));
        });

        return MemdexPath.build(
                planet.getProperty(PATH).toString(),
                attributes.build(),
                counters.build(),
                memdexpaths.build());
    }

    private static final class Request {
        public final Map<String, String> staticAttributes;
        public final Set<String> dynamicAttributes;

        public Request(
                @JsonProperty("staticAttributes") Map<String, String> staticAttributes,
                @JsonProperty("dynamicAttributes") Set<String> dynamicAttributes) {
            this.staticAttributes = staticAttributes;
            this.dynamicAttributes = dynamicAttributes;
        }
    }

    private static final class MemdexPathWithRealm {
        public final String realm;
        public final MemdexPath memdexPath;

        public MemdexPathWithRealm(
                @JsonProperty("realm") String realm,
                @JsonProperty("memdexPath") MemdexPath memdexPath) {
            this.realm = realm;
            this.memdexPath = memdexPath;
        }
    }

}
