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
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;

class IwanMappingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IwanMappingStrategy.class);

    private final ImmutableMap<String, Integer> columnIndexes;
    private final ImmutableMultimap<String, HeaderElement> mapping;

    IwanMappingStrategy(ImmutableMap<String, Integer> columnIndexes, ImmutableMultimap<String, HeaderElement> mapping) {
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
            columnIndexesBldr.put(he.columnIdentifier(), index);
            index++;
        }

        ImmutableMultimap<String, HeaderElement> mapping = mappingBldr.build();
        LOGGER.debug(Arrays.toString(mapping.keySet().toArray(new String[mapping.keySet().size()])));


        return new IwanMappingStrategy(columnIndexesBldr.build(), mapping);
    }

    LineMappingStrategy reduceStrategyForLine(Set<String> scopesTypes, String[] line) {
        ImmutableMap.Builder<String, Integer> newIndex = ImmutableMap.builder();
        ImmutableMultimap.Builder<String, HeaderElement> newMapping = ImmutableMultimap.builder();
        ImmutableSet.Builder<String> empties = ImmutableSet.builder();
        mapping.values().forEach(he -> {
            String value = line[he.index];
            if (value != null && !value.trim().isEmpty()) {
                newIndex.put(he.columnIdentifier(), he.index);
                newMapping.put(he.elementName, he);
            } else if (TAG.equals(he.propertyName)) {
                empties.add(he.elementName);
            }
        });

        Scope scope = IWanLoaderHelper.findScopeValue(this, scopesTypes, line).orElse(null);

        return new LineMappingStrategy(scope, newIndex.build(), newMapping.build(), empties.build());
    }

    ImmutableCollection<HeaderElement> getElementHeaders(String name) {
        return mapping.get(name);
    }

    int getColumnIndex(String keyType, String property) {
        return tryColumnIndex(keyType, property)
                .orElseThrow(() -> {
                    String column = keyType + '.' + property;
                    return new NoSuchElementException(String.format("Required column '%s' not found.", column));
                });
    }

    Optional<Integer> tryColumnIndex(String keyType, String property) {
        String column = keyType + '.' + property;
        return Optional.ofNullable(columnIndexes.get(column));
    }

    ImmutableSet<String> guessKeyTypesForLine(Collection<String> scopeTypes, String[] line) {
        Set<String> scopeKeyTypes = scopeTypes.stream()
                .filter(this::hasKeyType)
                .filter(skt -> {
                    int id = getColumnIndex(skt, "id");
                    return id >= 0 && line[id] != null && !line[id].isEmpty();

                }).collect(Collectors.toSet());

        return ImmutableSet.copyOf(scopeKeyTypes);
    }

    ImmutableMap<String, Set<String>> guessElementCreationStrategy(Collection<String> scopeKeyTypes, Map<String, ? extends List<Relationship>> children) {
        Map<String, Set<String>> collect = Maps.newHashMap();
        scopeKeyTypes.forEach(s ->
                collect.putAll(addChildrenAttribute(s, collect, children)));

        if (collect.isEmpty()) {
            collect.putAll(addChildrenAttribute(GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE, collect, children));
        }
        mapping.keySet().stream()
                .filter(k -> !collect.keySet().contains(k))
                .forEach(k -> collect.putAll(addChildrenAttribute(k, collect, children)));

        return ImmutableMap.copyOf(collect);
    }

    ImmutableList<MultiElementHeader> getMultiElementHeader() {
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
                })).filter(Objects::nonNull)
                .collect(Collectors.toList());
        return ImmutableList.copyOf(collect);
    }

    boolean hasKeyType(String keytype) {
        return mapping.keySet().contains(keytype);
    }

    private Map<String, Set<String>> addChildrenAttribute(
            String current, Map<String, Set<String>> collect, Map<String, ? extends List<Relationship>> children) {

        Collection<Relationship> relationships = children.get(current);
        if (relationships == null) return ImmutableMap.of();

        if (!relationships.isEmpty()) {
            for (Relationship relationship : relationships) {
                Set<String> p = collect.computeIfAbsent(current, k -> Sets.newHashSet());

                Node startNode = relationship.getStartNode();
                String type = startNode.getProperty(GraphModelConstants._TYPE).toString();
                String name = startNode.getProperty(GraphModelConstants.NAME).toString();
                String key = type + GraphModelConstants.KEYTYPE_SEPARATOR + name;
                if (mapping.keySet().contains(key)) {
                    p.add(key);
                    addChildrenAttribute(key, collect, children).forEach((k, v) -> {
                        Set<String> values = collect.computeIfAbsent(k, k1 -> Sets.newHashSet());
                        values.addAll(v);
                    });
                }
            }
        } else {
            collect.put(current, Sets.newHashSet());
        }
        return collect;
    }

    ImmutableSet<String> getAllElementsType() {
        return mapping.keySet();
    }
}
