package com.livingobjects.neo4j.iwan;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.Set;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.KEYTYPE_SEPARATOR;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE;

final class IwanMappingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IwanMappingStrategy.class);
    private static final String _TYPE = "_type";
    private static final String NAME = "name";

    private final ImmutableMultimap<String, HeaderElement> mapping;

    private IwanMappingStrategy(ImmutableMultimap<String, HeaderElement> mapping) {
        this.mapping = mapping;
    }

    static IwanMappingStrategy captureHeader(CSVReader reader) throws IOException {
        String[] headers = reader.readNext();
        ImmutableMultimap.Builder<String, HeaderElement> mappingBldr = ImmutableMultimap.builder();

        LOGGER.debug("header : " + Arrays.toString(headers));

        int idx = 0;
        for (String header : headers) {
            HeaderElement he = HeaderElement.of(header, idx++);
            mappingBldr.put(he.elementName, he);
        }

        ImmutableMultimap<String, HeaderElement> mapping = mappingBldr.build();
        LOGGER.debug(Arrays.toString(mapping.keySet().toArray(new String[mapping.keySet().size()])));


        return new IwanMappingStrategy(mapping);
    }

    ImmutableCollection<HeaderElement> getElementHeaders(String name) {
        return mapping.get(name);
    }

    ImmutableMap<String, Set<String>> guessElementCreationStrategy(List<String> scopes, Map<String, ? extends List<Relationship>> children) {
        Map<String, Set<String>> collect = Maps.newHashMap();
        scopes.stream().filter(this::isScope).forEach(s ->
                collect.putAll(addChildrenAttribute(s, collect, children)));

        if (collect.isEmpty()) {
            collect.putAll(addChildrenAttribute(SCOPE_GLOBAL_ATTRIBUTE, collect, children));
        }

        return ImmutableMap.copyOf(collect);
    }

    final boolean isScope(String keytype) {
        return mapping.keySet().contains(keytype);
    }

    private Map<String, Set<String>> addChildrenAttribute(
            String current, Map<String, Set<String>> collect, Map<String, ? extends List<Relationship>> children) {

        Collection<Relationship> relationships = children.get(current);
        if (!relationships.isEmpty()) {
            for (Relationship relationship : relationships) {
                Set<String> p = collect.get(current);
                if (p == null) {
                    p = Sets.newHashSet();
                    collect.put(current, p);
                }

                Node startNode = relationship.getStartNode();
                String type = startNode.getProperty(_TYPE).toString();
                String name = startNode.getProperty(NAME).toString();
                String key = type + KEYTYPE_SEPARATOR + name;
                if (mapping.keySet().contains(key)) {
                    p.add(key);
                    addChildrenAttribute(key, collect, children).forEach((k, v) -> {
                        Set<String> values = collect.get(k);
                        if (values == null) {
                            values = Sets.newHashSet();
                            collect.put(k, values);
                        }
                        values.addAll(v);
                    });
                }
            }
        } else {
            collect.put(current, Sets.newHashSet());
        }
        return collect;
    }
}
