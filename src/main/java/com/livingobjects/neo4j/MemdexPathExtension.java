package com.livingobjects.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.iwan.model.MemdexPath;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.*;

@Path("/memdexpath")
public final class MemdexPathExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemdexPathExtension.class);

    private final GraphDatabaseService graphDb;
    private final ObjectMapper json = new ObjectMapper();

    public MemdexPathExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @POST
    public Response memdexPath(InputStream request) throws IOException, ServletException {
        // List of set of attributes as _type:value
        List<Set<String>> filters = json.readValue(request, new TypeReference<List<Set<String>>>() {
        });

        StreamingOutput stream = outputStream -> {
            JsonGenerator jg = json.getJsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
            jg.writeStartArray();

            try (Transaction ignored = graphDb.beginTx()) {
                for (Set<String> filter : filters) {
                    StringBuilder query = new StringBuilder(200);
                    Map<String, Object> binds = Maps.newHashMapWithExpectedSize((filter.size() * 2) + 1);
                    query.append("MATCH (p:Planet)-[:Attribute]->(r:Attribute {_type:{realmType}})\n" +
                            "WHERE ");
                    binds.put("realmType", "realm");

                    int i = 0;
                    for (String attribute : filter) {
                        String[] split = attribute.split(":");
                        query.append("(p)-[:Attribute]->(:Attribute {_type:{aType").append(i).append("},name:{aName").append(i).append("}})\nAND ");
                        binds.put("aType" + i, split[0]);
                        binds.put("aName" + i, split[1]);
                        i++;
                    }

                    query.setLength(query.length() - 4);
                    query.append("RETURN r, p");

                    Result result = graphDb.execute(query.toString(), binds);
                    String realm;
                    Node firstPlanet;
                    if (result.hasNext()) {
                        Map<String, Object> next = result.next();
                        if (result.hasNext()) {
                            LOGGER.error("Too many realm for filter {}", filter);
                        }
                        Node rNode = (Node) next.get("r");
                        firstPlanet = (Node) next.get("p");
                        realm = rNode.getProperty(NAME).toString();
                    } else {
                        LOGGER.error("No realm found for filter {}", filter);
                        continue;
                    }
                    MemdexPath memdexPath = browsePlanetToMemdexPath(firstPlanet);

                    jg.writeObject(Maps.immutableEntry(realm, memdexPath));
                    jg.flush();
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
                planet.getProperty(PATH).toString(),
                attributes.build(),
                counters.build(),
                memdexpaths.build());
    }
}
