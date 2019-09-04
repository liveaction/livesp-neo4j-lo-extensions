package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.export.query.ExportQuery;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

public final class Lineages {

    private static final Set<String> METADATA_PROPERTIES = ImmutableSet.of("tag", "createdAt", "updatedAt", "createdBy", "updatedBy");

    public final Set<Lineage> lineages;

    public final Set<Node> visitedNodes;

    public final Map<String, SortedMap<String, String>> propertiesTypeByType;

    public final ImmutableSet<String> attributesToExtract;
    public final ImmutableSet<String> attributesToExport;
    public final ImmutableSet<String> orderedLeafAttributes;

    private final ImmutableMap<String, Set<String>> columnsToExport;
    private final boolean includeMetadata;

    private boolean recursiveLineageComparator(MetaSchema metaSchema, String o1, String o2) {
        if(metaSchema.getMonoParentRelations(o1)
                .anyMatch(o2::equals)) {
            return true;
        } else {
            return metaSchema.getMonoParentRelations(o1)
                    .anyMatch(s -> recursiveLineageComparator(metaSchema, s, o2));
        }
    }

    public Lineages(MetaSchema metaSchema, ExportQuery exportQuery) {
        Comparator<String> lineageComparator = (o1, o2) -> {
            if (recursiveLineageComparator(metaSchema, o1, o2)) {
                return 1;
            } else if (recursiveLineageComparator(metaSchema, o2, o1)) {
                return -1;
            } else {
                return o1.compareTo(o2);
            }
        };

        Set<String> attributesToExport = Sets.newTreeSet(lineageComparator);
        attributesToExport.addAll(exportQuery.parentAttributes);
        attributesToExport.addAll(exportQuery.requiredAttributes);
        this.attributesToExport = ImmutableSet.copyOf(attributesToExport);

        Set<String> filterKeyAttributes = exportQuery.filter.columns()
                .stream()
                .map(c -> c.keyAttribute)
                .collect(Collectors.toSet());

        Set<String> attributesToExtract = Sets.newTreeSet(lineageComparator);
        attributesToExtract.addAll(exportQuery.parentAttributes);
        attributesToExtract.addAll(exportQuery.requiredAttributes);
        attributesToExtract.addAll(filterKeyAttributes);
        this.attributesToExtract = ImmutableSet.copyOf(attributesToExtract);

        Set<String> orderedLeafAttributes = Sets.newTreeSet((o1, o2) -> -lineageComparator.compare(o1, o2));
        orderedLeafAttributes.addAll(exportQuery.requiredAttributes);
        orderedLeafAttributes.addAll(filterKeyAttributes);
        this.orderedLeafAttributes = ImmutableSet.copyOf(orderedLeafAttributes);

        Comparator<Lineage> comparator = new LineageSortComparator(exportQuery.sort, new LineageNaturalComparator(this.attributesToExport));
        lineages = Sets.newTreeSet(comparator);
        visitedNodes = Sets.newHashSet();
        propertiesTypeByType = Maps.newHashMap();
        includeMetadata = exportQuery.includeMetadata;
        columnsToExport = exportQuery.columns;

        addScopeColumn(this.attributesToExport, metaSchema);
    }

    private void addScopeColumn(ImmutableSet<String> attributesToExport, MetaSchema metaSchema) {
        for (String keyAttribute : attributesToExport) {
            if (metaSchema.isMultiScope(keyAttribute) && filterColumn(keyAttribute, GraphModelConstants.SCOPE)) {
                SortedMap<String, String> keyAttributeProperties = getKeyAttributeProperties(keyAttribute);
                keyAttributeProperties.put(GraphModelConstants.SCOPE, "STRING");
            }
        }
    }

    public boolean dejaVu(Node leaf) {
        return visitedNodes.contains(leaf);
    }

    public void markAsVisited(String keyAttribute, Node node) {
        visitedNodes.add(node);
        if (attributesToExtract.contains(keyAttribute)) {
            SortedMap<String, String> properties = getKeyAttributeProperties(keyAttribute);
            for (Map.Entry<String, Object> property : node.getAllProperties().entrySet()) {
                String name = property.getKey();
                if (!name.startsWith("_") && !ignoreProperty(name) && filterColumn(keyAttribute, name)) {
                    String propertyType = PropertyConverter.getPropertyType(property.getValue());
                    properties.put(name, propertyType);
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

    private boolean filterColumn(String keyAttribute, String name) {
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

    private SortedMap<String, String> getKeyAttributeProperties(String keyAttribute) {
        return propertiesTypeByType.computeIfAbsent(keyAttribute, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
    }

}
