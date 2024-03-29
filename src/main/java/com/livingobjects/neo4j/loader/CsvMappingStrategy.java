package com.livingobjects.neo4j.loader;

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
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class CsvMappingStrategy {

    private final ImmutableMap<String, Integer> columnIndexes;
    private final ImmutableMultimap<String, HeaderElement> mapping;
    private final MetaSchema metaSchema;

    CsvMappingStrategy(ImmutableMap<String, Integer> columnIndexes, ImmutableMultimap<String, HeaderElement> mapping, MetaSchema metaSchema) {
        this.columnIndexes = columnIndexes;
        this.mapping = mapping;
        this.metaSchema = metaSchema;
    }

    static CsvMappingStrategy captureHeader(CSVReader reader, MetaSchema metaSchema) throws IOException, CsvValidationException {
        String[] headers = reader.readNext();
        ImmutableMap.Builder<String, Integer> columnIndexesBldr = ImmutableMap.builder();
        ImmutableMultimap.Builder<String, HeaderElement> mappingBldr = ImmutableMultimap.builder();

        int index = 0;
        for (String header : headers) {
            HeaderElement he = HeaderElement.of(header, index);
            mappingBldr.put(he.elementName, he);
            columnIndexesBldr.put(he.columnIdentifier(), index);
            index++;
        }

        ImmutableMultimap<String, HeaderElement> mapping = mappingBldr.build();
        return new CsvMappingStrategy(columnIndexesBldr.build(), mapping, metaSchema);
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

    ImmutableMap<String, Set<String>> guessElementCreationStrategy(Collection<String> scopeKeyTypes, Transaction tx) {
        Map<String, Set<String>> collect = Maps.newHashMap();

        scopeKeyTypes.forEach(s -> addChildrenAttribute(s, collect, tx));

        if (collect.isEmpty()) {
            addChildrenAttribute(GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE, collect, tx);
        }
        mapping.keySet()
                .forEach(k -> addChildrenAttribute(k, collect, tx));

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
            String current, Map<String, Set<String>> collect, Transaction tx) {

        Collection<Relationship> relationships = metaSchema.getChildren(tx, current);

        if (!relationships.isEmpty()) {
            for (Relationship relationship : relationships) {
                Set<String> p = collect.computeIfAbsent(current, k -> Sets.newHashSet());

                Node startNode = relationship.getStartNode();
                String type = startNode.getProperty(GraphModelConstants._TYPE).toString();
                String name = startNode.getProperty(GraphModelConstants.NAME).toString();
                String key = type + GraphModelConstants.KEYTYPE_SEPARATOR + name;
                if (mapping.keySet().contains(key)) {
                    p.add(key);
                    addChildrenAttribute(key, collect, tx).forEach((k, v) -> {
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
