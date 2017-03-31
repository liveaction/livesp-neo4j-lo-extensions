package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.header.HeaderElement;
import com.livingobjects.neo4j.model.header.HeaderElement.Visitor;
import com.livingobjects.neo4j.model.header.MultiElementHeader;
import com.livingobjects.neo4j.model.header.SimpleElementHeader;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
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
import java.util.stream.Collectors;

final class IwanMappingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IwanMappingStrategy.class);

    private final ImmutableMap<String, Integer> columnIndexes;
    private final ImmutableMultimap<String, HeaderElement> mapping;

    private IwanMappingStrategy(ImmutableMap<String, Integer> columnIndexes, ImmutableMultimap<String, HeaderElement> mapping) {
        this.columnIndexes = columnIndexes;
        this.mapping = mapping;
    }

    static IwanMappingStrategy captureHeader(CSVReader reader) throws IOException {
        String[] headers = reader.readNext();
        ImmutableMap.Builder<String, Integer> columnIndexesBldr = ImmutableMap.builder();
        ImmutableMultimap.Builder<String, HeaderElement> mappingBldr = ImmutableMultimap.builder();

        LOGGER.debug("header : " + Arrays.toString(headers));

        int index = 0;
        for (String header : headers) {
            HeaderElement he = HeaderElement.of(header, index);
            mappingBldr.put(he.elementName, he);
            columnIndexesBldr.put(he.elementName + '.' + he.propertyName, index);
            index++;
        }

        ImmutableMultimap<String, HeaderElement> mapping = mappingBldr.build();
        LOGGER.debug(Arrays.toString(mapping.keySet().toArray(new String[mapping.keySet().size()])));


        return new IwanMappingStrategy(columnIndexesBldr.build(), mapping);
    }

    ImmutableCollection<HeaderElement> getElementHeaders(String name) {
        return mapping.get(name);
    }

    int getColumnIndex(String keyType, String property) {
        String column = keyType + '.' + property;
        Integer index = columnIndexes.get(column);
        if (index == null) {
            throw new IllegalArgumentException(String.format("Required column '%s' not found.", column));
        }
        return index;
    }

    ImmutableMap<String, Set<String>> guessElementCreationStrategy(Collection<String> scopeKeyTypes, Map<String, ? extends List<Relationship>> children) {
        Map<String, Set<String>> collect = Maps.newHashMap();
        scopeKeyTypes.forEach(s ->
                collect.putAll(addChildrenAttribute(s, collect, children)));

        if (collect.isEmpty()) {
            collect.putAll(addChildrenAttribute(IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE, collect, children));
            mapping.keySet().stream()
                    .filter(k -> !collect.keySet().contains(k))
                    .forEach(k -> collect.putAll(addChildrenAttribute(k, collect, children)));
        }

        return ImmutableMap.copyOf(collect);
    }

    public ImmutableList<MultiElementHeader> getMultiElementHeader() {
        List<MultiElementHeader> collect = mapping.values().stream()
                .map(h -> h.visit(new Visitor<MultiElementHeader>() {
                    @Override
                    public MultiElementHeader visitSimple(SimpleElementHeader header) {
                        return null;
                    }

                    @Override
                    public MultiElementHeader visitMulti(MultiElementHeader header) {
                        return header;
                    }
                })).filter(h -> h != null)
                .collect(Collectors.toList());
        return ImmutableList.copyOf(collect);
    }

    final boolean hasKeyType(String keytype) {
        return mapping.keySet().contains(keytype);
    }

    private Map<String, Set<String>> addChildrenAttribute(
            String current, Map<String, Set<String>> collect, Map<String, ? extends List<Relationship>> children) {

        Collection<Relationship> relationships = children.get(current);
        assert relationships != null : "Current type " + current + " does not exists !";

        if (!relationships.isEmpty()) {
            for (Relationship relationship : relationships) {
                Set<String> p = collect.get(current);
                if (p == null) {
                    p = Sets.newHashSet();
                    collect.put(current, p);
                }

                Node startNode = relationship.getStartNode();
                String type = startNode.getProperty(IwanModelConstants._TYPE).toString();
                String name = startNode.getProperty(IwanModelConstants.NAME).toString();
                String key = type + IwanModelConstants.KEYTYPE_SEPARATOR + name;
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

    public ImmutableSet<String> getAllElementsType() {
        return mapping.keySet();
    }
}
