package com.livingobjects.neo4j.iwan;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.livingobjects.neo4j.iwan.model.HeaderElement;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.livingobjects.cosmos.shared.model.GraphNodeProperties.NAME;
import static com.livingobjects.cosmos.shared.model.GraphNodeProperties._TYPE;

public final class IwanMappingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IwanMappingStrategy.class);

    private final ImmutableMultimap<String, HeaderElement> mapping;

    private IwanMappingStrategy(ImmutableMultimap<String, HeaderElement> mapping) {
        this.mapping = mapping;
    }

    public static IwanMappingStrategy captureHeader(CSVReader reader) throws IOException {
        String[] headers = reader.readNext();
        ImmutableMultimap.Builder<String, HeaderElement> mappingBldr = ImmutableMultimap.builder();

        LOGGER.debug("header : " + Arrays.toString(headers));

        int idx = 0;
        for (String header : headers) {
            HeaderElement he = HeaderElement.of(header, idx++);
            mappingBldr.put(he.elementName, he);
        }

        ImmutableMultimap<String, HeaderElement> mapping = mappingBldr.build();
        LOGGER.warn(Arrays.toString(mapping.keySet().toArray(new String[mapping.size()])));


        return new IwanMappingStrategy(mapping);
    }

    public ImmutableCollection<HeaderElement> getElementHeaders(String name) {
        return mapping.get(name);
    }

    public ImmutableMap<String, List<String>> guessElementCreationStrategy(List<String> scopes, Map<String, ? extends List<Relationship>> children) {
        Map<String, List<String>> collect = Maps.newHashMap();
        scopes.stream().filter(this::isScope).forEach(s -> {
            collect.putAll(addChildrenAttribute(s, collect, children));
        });

        return ImmutableMap.copyOf(collect);
    }

    public final boolean isScope(String keytype) {
        return mapping.keySet().contains(keytype);
    }

    private Map<String, List<String>> addChildrenAttribute(
            String current, Map<String, List<String>> collect, Map<String, ? extends List<Relationship>> children) {

        Collection<Relationship> relationships = children.get(current);
        if (!relationships.isEmpty()) {
            for (Relationship relationship : relationships) {
                Node startNode = relationship.getStartNode();
                String type = startNode.getProperty(_TYPE).toString();
                String name = startNode.getProperty(NAME).toString();
                String key = type + ':' + name;
                if (mapping.keySet().contains(key)) {
                    List<String> p = collect.get(current);
                    if (p == null) {
                        p = Lists.newArrayList();
                        collect.put(current, p);
                    }
                    p.add(key);
                    return addChildrenAttribute(key, collect, children);
                }
            }
        } else {
            collect.put(current, Lists.newArrayList());
        }
        return collect;
    }
}
