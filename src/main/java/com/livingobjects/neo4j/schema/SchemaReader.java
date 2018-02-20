package com.livingobjects.neo4j.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.KEYTYPE_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.LINK_PROP_SPECIALIZER;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._TYPE;

public class SchemaReader {

    /**
     * @param segment The first segment
     * @return One MemdexPath entry with all its counters
     */
    public static Map.Entry<MemdexPathNode, Map<String, Node>> browseSegments(Node segment) {
        Map<String, Node> countersDictionary = Maps.newHashMap();

        String segmentName = segment.getProperty("path").toString();

        List<String> counters = Lists.newArrayList();
        segment.getRelationships(RelationshipTypes.PROVIDED, Direction.INCOMING).forEach(link -> {
            Node counterNode = link.getStartNode();
            if (!counterNode.hasProperty("name") || !link.hasProperty("context")) return;

            String counterRef = "kpi:" + counterNode.getProperty("name") + "@context:" + link.getProperty("context");
            counters.add(counterRef);
            countersDictionary.putIfAbsent(counterRef, counterNode);
        });

        List<MemdexPathNode> children = Lists.newArrayList();
        segment.getRelationships(RelationshipTypes.MEMDEXPATH, Direction.OUTGOING).forEach(path -> {
            Map.Entry<MemdexPathNode, Map<String, Node>> entry = browseSegments(path.getEndNode());
            children.add(entry.getKey());
            countersDictionary.putAll(entry.getValue());
        });

        List<String> strings = browseAttributes(segment);
        return Maps.immutableEntry(new MemdexPathNode(
                segmentName, strings.get(0), counters, children
        ), countersDictionary);
    }

    private static List<String> browseAttributes(Node node) {
        List<String> attributes = Lists.newArrayList();
        node.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING).forEach(link -> {
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

}
