package com.livingobjects.neo4j.model.export;

import com.google.common.collect.*;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;

import java.util.*;
import java.util.stream.Collectors;

public final class Lineages {

    private static final Set<String> METADATA_PROPERTIES = ImmutableSet.of("tag", "createdAt", "updatedAt", "createdBy", "updatedBy");

    private final List<Lineage> lineages;

    public final Set<Node> visitedNodes;

    public final Map<String, SortedMap<String, String>> propertiesTypeByType;

    public final ImmutableSet<String> attributesToExtract;
    public final ImmutableSet<String> attributesToExport;
    public final ImmutableSet<String> orderedLeafAttributes;

    private final ImmutableMap<String, Set<String>> columnsToExport;
    private final boolean includeMetadata;
    private final ImmutableMap<String, Optional<ImmutableSet<String>>> propertiesToExtractByType;
    private final Comparator<Lineage> lineageSortComparator;


    public Lineages(MetaSchema metaSchema, ExportQuery exportQuery) {

        Set<String> attributesToExport = Sets.newTreeSet(metaSchema.lineageComparator);
        attributesToExport.addAll(exportQuery.parentAttributes);
        attributesToExport.addAll(exportQuery.requiredAttributes);
        this.attributesToExport = ImmutableSet.copyOf(attributesToExport);

        Set<String> filterKeyAttributes = exportQuery.filter.columns()
                .stream()
                .map(c -> c.keyAttribute)
                .collect(Collectors.toSet());

        Set<String> attributesToExtract = Sets.newTreeSet(metaSchema.lineageComparator);
        attributesToExtract.addAll(exportQuery.parentAttributes);
        attributesToExtract.addAll(exportQuery.requiredAttributes);
        attributesToExtract.addAll(filterKeyAttributes);
        this.attributesToExtract = ImmutableSet.copyOf(attributesToExtract);

        Set<String> orderedLeafAttributes = Sets.newTreeSet((o1, o2) -> -metaSchema.lineageComparator.compare(o1, o2));
        orderedLeafAttributes.addAll(exportQuery.requiredAttributes);
        orderedLeafAttributes.addAll(filterKeyAttributes);
        this.orderedLeafAttributes = ImmutableSet.copyOf(orderedLeafAttributes);

        Map<String, Optional<Set<String>>> builder = new HashMap<>();
        exportQuery.columns.forEach((att, set) -> builder.put(att, Optional.of(new HashSet<>(set))));
        attributesToExtract.forEach(att -> builder.computeIfAbsent(att, unused -> Optional.empty()));
        exportQuery.filter.columns().forEach(column -> builder.computeIfAbsent(column.keyAttribute, unused -> Optional.of(Sets.newHashSet())).ifPresent(s -> s.add(column.property)));
        exportQuery.sort.forEach(columnOrder -> builder.computeIfAbsent(columnOrder.column.keyAttribute, unused -> Optional.of(Sets.newHashSet())).ifPresent(s -> s.add(columnOrder.column.property)));

        this.propertiesToExtractByType = builder.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().map(ImmutableSet::copyOf)))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        lineageSortComparator = new LineageSortComparator(exportQuery.sort, new LineageNaturalComparator(this.attributesToExport));
        lineages = new ArrayList<>();
        visitedNodes = new HashSet<>();
        propertiesTypeByType = new HashMap<>();
        includeMetadata = exportQuery.includeMetadata;
        columnsToExport = exportQuery.columns;

        addScopeColumn(this.attributesToExport, metaSchema);
    }

    public Lineages(MetaSchema metaSchema, ExportQuery exportQuery, Set<String> commonChilds) {

        Set<String> attributesToExport = Sets.newTreeSet(metaSchema.lineageComparator);
        attributesToExport.addAll(exportQuery.parentAttributes);
        attributesToExport.addAll(exportQuery.requiredAttributes);
        this.attributesToExport = ImmutableSet.copyOf(attributesToExport);

        Set<String> filterKeyAttributes = exportQuery.filter.columns()
                .stream()
                .map(c -> c.keyAttribute)
                .collect(Collectors.toSet());

        Set<String> attributesToExtract = Sets.newTreeSet(metaSchema.lineageComparator);
        attributesToExtract.addAll(exportQuery.parentAttributes);
        attributesToExtract.addAll(exportQuery.requiredAttributes);
        attributesToExtract.addAll(filterKeyAttributes);
        this.attributesToExtract = ImmutableSet.copyOf(attributesToExtract);

        Set<String> orderedLeafAttributes = Sets.newTreeSet((o1, o2) -> -metaSchema.lineageComparator.compare(o1, o2));
        orderedLeafAttributes.addAll(commonChilds);
        orderedLeafAttributes.addAll(exportQuery.requiredAttributes);
        orderedLeafAttributes.addAll(filterKeyAttributes);
        this.orderedLeafAttributes = ImmutableSet.copyOf(orderedLeafAttributes);

        Map<String, Optional<Set<String>>> builder = new HashMap<>();
        exportQuery.columns.forEach((att, set) -> builder.put(att, Optional.of(new HashSet<>(set))));
        attributesToExtract.forEach(att -> builder.computeIfAbsent(att, unused -> Optional.empty()));
        exportQuery.filter.columns().forEach(column -> builder.computeIfAbsent(column.keyAttribute, unused -> Optional.of(Sets.newHashSet())).ifPresent(s -> s.add(column.property)));
        exportQuery.sort.forEach(columnOrder -> builder.computeIfAbsent(columnOrder.column.keyAttribute, unused -> Optional.of(Sets.newHashSet())).ifPresent(s -> s.add(columnOrder.column.property)));

        this.propertiesToExtractByType = builder.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().map(ImmutableSet::copyOf)))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        lineageSortComparator = new LineageSortComparator(exportQuery.sort, new LineageNaturalComparator(Sets.union(this.attributesToExport, filterKeyAttributes).immutableCopy()));
        lineages = new ArrayList<>();
        visitedNodes = new HashSet<>();
        propertiesTypeByType = new HashMap<>();
        includeMetadata = exportQuery.includeMetadata;
        columnsToExport = exportQuery.columns;

        addScopeColumn(this.attributesToExport, metaSchema);
    }

    public Optional<ImmutableSet<String>> getPropertiesToExtract(String type) {
        return propertiesToExtractByType.get(type);
    }

    private void addScopeColumn(ImmutableSet<String> attributesToExport, MetaSchema metaSchema) {
        for (String keyAttribute : attributesToExport) {
            if (metaSchema.isMultiScope(keyAttribute) && filterColumn(keyAttribute, GraphModelConstants.SCOPE)) {
                SortedMap<String, String> keyAttributeProperties = getKeyAttributePropertiesType(keyAttribute);
                keyAttributeProperties.put(GraphModelConstants.SCOPE, "STRING");
            }
        }
    }

    public boolean dejaVu(Node leaf) {
        return visitedNodes.contains(leaf);
    }

    public void markAsVisited(String keyAttribute, Node node, Lineage lineage) {
        visitedNodes.add(node);
        if (attributesToExtract.contains(keyAttribute)) {
            SortedMap<String, String> properties = getKeyAttributePropertiesType(keyAttribute);
            Map<String, Object> values = lineage.propertiesByType.computeIfAbsent(keyAttribute, k -> new HashMap<>());
            Optional<ImmutableSet<String>> propertiesToExtract = getPropertiesToExtract(keyAttribute);
            Map<String, Object> currentProperties = propertiesToExtract
                    .map(pTE -> node.getProperties(pTE.toArray(new String[0])))
                    .orElseGet(node::getAllProperties);
            for (Map.Entry<String, Object> property : currentProperties.entrySet()) {
                String name = property.getKey();
                if (!ignoreProperty(name)) {
                    if (filterColumn(keyAttribute, name)) {
                        properties.put(name, PropertyConverter.getPropertyType(property.getValue()));
                    }
                    if (propertiesToExtract.map(set -> set.contains(name)).orElse(true)) {
                        values.put(name, property.getValue());
                    }
                }
            }
        }
    }

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
        return ImmutableSortedSet.copyOf(lineageSortComparator, lineages);
    }

    private SortedMap<String, String> getKeyAttributePropertiesType(String keyAttribute) {
        return propertiesTypeByType.computeIfAbsent(keyAttribute, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
    }


}
