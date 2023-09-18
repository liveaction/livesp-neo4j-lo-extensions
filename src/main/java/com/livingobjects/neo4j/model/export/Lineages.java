package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class Lineages {

    private static final Set<String> METADATA_PROPERTIES = ImmutableSet.of("tag", "createdAt", "updatedAt", "createdBy", "updatedBy");

    public Map<String, Map<String, String>> propertiesTypeByType;
    public final ImmutableSet<String> attributesToExtract;
    public final ImmutableSortedSet<String> attributesToExport;
    public final ImmutableSet<String> orderedLeafAttributes;
    private final Transaction tx;
    public final boolean noResult;
    public final boolean parentsCardinality;

    private final List<Lineage> lineages;
    private final Set<Node> visitedNodes;
    private final ImmutableMap<String, Set<String>> columnsToExport;
    private final boolean includeMetadata;

    public Lineages(Transaction tx, MetaSchema metaSchema, ExportQuery exportQuery, Set<String> commonChilds, ImmutableList<String> relAttrToExport) {
        this.tx = tx;
        this.noResult = exportQuery.noResult;

        this.parentsCardinality = exportQuery.parentsCardinality;

        Set<String> attributesToExport = Sets.union(exportQuery.parentAttributes, exportQuery.requiredAttributes);
        Comparator<String> lineageComparator = metaSchema.getLineageComparator(tx);
        this.attributesToExport = ImmutableSortedSet.copyOf(lineageComparator, attributesToExport);

        Set<String> filterKeyAttributes = exportQuery.filter.columns()
                .stream()
                .map(c -> c.keyAttribute)
                .collect(Collectors.toSet());

        Set<String> attributesToExtract = Sets.newTreeSet(lineageComparator);
        attributesToExtract.addAll(exportQuery.parentAttributes);
        attributesToExtract.addAll(exportQuery.requiredAttributes);
        attributesToExtract.addAll(filterKeyAttributes);
        attributesToExtract.addAll(relAttrToExport);
        this.attributesToExtract = ImmutableSet.copyOf(attributesToExtract);

        Set<String> orderedLeafAttributes = Sets.newTreeSet((o1, o2) -> -lineageComparator.compare(o1, o2));
        orderedLeafAttributes.addAll(commonChilds);
        orderedLeafAttributes.addAll(exportQuery.requiredAttributes);
        orderedLeafAttributes.addAll(filterKeyAttributes);
        this.orderedLeafAttributes = ImmutableSet.copyOf(orderedLeafAttributes);

        Map<String, Optional<Set<String>>> builder = new HashMap<>();
        exportQuery.columns.forEach((att, set) -> builder.put(att, Optional.of(new HashSet<>(set))));
        attributesToExtract.forEach(att -> builder.computeIfAbsent(att, unused -> Optional.empty()));
        exportQuery.filter.columns().forEach(column -> builder.computeIfAbsent(column.keyAttribute, unused -> Optional.of(Sets.newHashSet())).ifPresent(s -> s.add(column.property)));

        lineages = new ArrayList<>();
        visitedNodes = new HashSet<>();
        propertiesTypeByType = new HashMap<>();
        includeMetadata = exportQuery.includeMetadata;
        columnsToExport = exportQuery.columns;

        addScopeColumn(this.attributesToExport, metaSchema);
    }

    private void addScopeColumn(ImmutableSet<String> attributesToExport, MetaSchema metaSchema) {
        for (String keyAttribute : attributesToExport) {
            if (metaSchema.isMultiScope(tx, keyAttribute) && filterColumn(keyAttribute, GraphModelConstants.SCOPE)) {
                getKeyAttributePropertiesType(keyAttribute).put(GraphModelConstants.SCOPE, "STRING");
            }
        }
    }

    public Optional<ImmutableSet<String>> getPropertiesToExport(String type) {
        return Optional.ofNullable(columnsToExport.get(type))
                .map(ImmutableSet::copyOf);
    }

    public boolean dejaVu(Node leaf) {
        return visitedNodes.contains(leaf);
    }

    public void markAsVisited(Node node) {
        visitedNodes.add(node);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean ignoreProperty(String name) {
        if (name.startsWith("_")) {
            return true;
        }
        if (METADATA_PROPERTIES.contains(name)) {
            return !includeMetadata;
        }
        return false;
    }

    public boolean filterColumn(String keyAttribute, String name) {
        if (columnsToExport.isEmpty()) {
            return true;
        }
        Set<String> toExport = columnsToExport.get(keyAttribute);
        if (toExport != null) {
            return toExport.contains(name);
        }
        return false;
    }

    public void add(Lineage lineage) {
        lineages.add(lineage);
    }

    public Set<Lineage> lineages() {
        return ImmutableSet.copyOf(lineages);
    }

    public Map<String, String> getKeyAttributePropertiesType(String keyAttribute) {
        return propertiesTypeByType.computeIfAbsent(keyAttribute, k -> new HashMap<>());
    }

    public void consolidatePropertiesTypeByType() {
        propertiesTypeByType = propertiesTypeByType.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey(PropertyNameComparator.PROPERTY_NAME_COMPARATOR))
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))));
    }
}
