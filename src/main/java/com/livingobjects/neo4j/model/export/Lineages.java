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
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

public final class Lineages {

    private static final Set<String> IGNORE = ImmutableSet.of("createdAt", "updatedAt", "createdBy", "updatedBy");

    private static final Set<String> IGNORE_WITH_TAGS = ImmutableSet.<String>builder()
            .addAll(IGNORE)
            .add("tag")
            .build();

    public final Set<Lineage> lineages;

    public final Set<Node> allTags;

    public final Map<String, SortedMap<String, String>> propertiesTypeByType;

    public final ImmutableSet<String> attributesToExport;
    public final ImmutableSet<String> orderedRequiredAttributes;

    private final Set<String> propertiesToIgnore;
    private final ImmutableMap<String, Set<String>> columns;

    public Lineages(MetaSchema metaSchema, ExportQuery exportQuery) {
        Comparator<String> lineageComparator = (o1, o2) -> {
            if (metaSchema.getMonoParentRelations(o1).anyMatch(o2::equals)) {
                return 1;
            } else if (metaSchema.getMonoParentRelations(o2).anyMatch(o1::equals)) {
                return -1;
            } else {
                return o1.compareTo(o2);
            }
        };

        Set<String> attributesToExport = Sets.newTreeSet(lineageComparator);
        attributesToExport.addAll(exportQuery.parentAttributes);
        attributesToExport.addAll(exportQuery.requiredAttributes);
        this.attributesToExport = ImmutableSet.copyOf(attributesToExport);

        Set<String> orderedRequiredAttributes = Sets.newTreeSet((o1, o2) -> -lineageComparator.compare(o1, o2));
        orderedRequiredAttributes.addAll(exportQuery.requiredAttributes);
        this.orderedRequiredAttributes = ImmutableSet.copyOf(orderedRequiredAttributes);

        Comparator<Lineage> comparator = new LineageSortComparator(exportQuery.sort, new LineageNaturalComparator(this.attributesToExport));
        lineages = Sets.newTreeSet(comparator);
        allTags = Sets.newHashSet();
        propertiesTypeByType = Maps.newHashMap();
        propertiesToIgnore = exportQuery.includeTag ? IGNORE : IGNORE_WITH_TAGS;
        columns = exportQuery.columns;
        addScopeColumn(this.attributesToExport, metaSchema);
    }

    private void addScopeColumn(ImmutableSet<String> attributesToExport, MetaSchema metaSchema) {
        for (String keyAttribute : attributesToExport) {
            if (metaSchema.isMultiScope(keyAttribute)) {
                SortedMap<String, String> keyAttributeProperties = getKeyAttributeProperties(keyAttribute);
                keyAttributeProperties.put(GraphModelConstants.SCOPE, "STRING");
            }
        }
    }

    public boolean dejaVu(Node leaf) {
        return allTags.contains(leaf);
    }

    public void markAsVisited(String keyAttribute, Node node) {
        allTags.add(node);
        if (attributesToExport.contains(keyAttribute)) {
            SortedMap<String, String> properties = getKeyAttributeProperties(keyAttribute);
            for (Map.Entry<String, Object> property : node.getAllProperties().entrySet()) {
                String name = property.getKey();
                if (!name.startsWith("_") && !propertiesToIgnore.contains(name) && filterColumn(keyAttribute, name)) {
                    String propertyType = PropertyConverter.getPropertyType(property.getValue());
                    properties.put(name, propertyType);
                }
            }
        }
    }

    private boolean filterColumn(String keyAttribute, String name) {
        return Optional.ofNullable(columns.get(keyAttribute))
                .map(c -> c.contains(name))
                .orElse(true);
    }

    public void add(Lineage lineage) {
        lineages.add(lineage);
    }

    private SortedMap<String, String> getKeyAttributeProperties(String keyAttribute) {
        return propertiesTypeByType.computeIfAbsent(keyAttribute, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
    }

}
