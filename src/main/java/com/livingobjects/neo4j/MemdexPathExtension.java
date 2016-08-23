package com.livingobjects.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.livingobjects.neo4j.iwan.model.IwanModelConstants;
import com.livingobjects.neo4j.iwan.model.MemdexPath;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import javax.servlet.ServletException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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
        Set<String> realms = json.readValue(request, new TypeReference<Set<String>>() {
        });

        StreamingOutput stream = outputStream -> {
            JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
            jg.writeStartArray();

            try (Transaction ignored = graphDb.beginTx()) {
                ResourceIterator<Node> realmNodes = graphDb.findNodes(IwanModelConstants.LABEL_ATTRIBUTE, _TYPE, "realm");
                while (realmNodes.hasNext()) {
                    Node realmNode = realmNodes.next();
                    if (!realms.contains(realmNode.getProperty(NAME).toString())) {
                        continue;
                    }

                    Node firstPlanet = realmNode.getSingleRelationship(LINK_ATTRIBUTE, Direction.INCOMING).getStartNode();
                    MemdexPath memdexPath = browsePlanetToMemdexPath(firstPlanet);
                    jg.writeObject(memdexPath);
                    jg.flush();
                    realms.remove(realmNode.getProperty(NAME).toString());
                }

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
                planet.getProperty(NAME).toString(),
                attributes.build(),
                counters.build(),
                memdexpaths.build());
    }


//    private MemdexPath fromPlanetToMemdexPath(Neo4jTx tx, Pair<Long, String> planet) throws Neo4jClientException {
//        Object[] neighbours = getNodeNeighbourById(tx, planet.first);
//
//        List<Pair<Long, String>> pPlanets = (List<Pair<Long, String>>) neighbours[1];
//        ImmutableList.Builder<MemdexPath> builder = ImmutableList.builder();
//
//        for (Pair<Long, String> nextPlanet : pPlanets) {
//            builder.add(fromPlanetToMemdexPath(tx, nextPlanet));
//        }
//
//        return MemdexPath.build(
//                planet.second,
//                (ImmutableList<Attribute>) neighbours[0],
//                (ImmutableList<CounterKPI>) neighbours[2],
//                builder.build());
//    }
}
