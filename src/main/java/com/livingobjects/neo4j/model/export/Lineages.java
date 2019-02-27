package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.loader.MetaSchema;
import com.livingobjects.neo4j.model.PropertyType;
import com.livingobjects.neo4j.model.export.query.ColumnOrder;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;

import java.util.Comparator;
import java.util.Map;
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

    private final Set<String> propertiesToIgnore;
    public final ImmutableList<String> attributesToExport;

    public Lineages(ImmutableList<String> attributesToExport, MetaSchema metaSchema, boolean exportTags, ImmutableList<ColumnOrder> sort) {
        this.attributesToExport = attributesToExport;
        Comparator<Lineage> comparator = new LineageSortComparator(sort, new LineageNaturalComparator(attributesToExport));
        lineages = Sets.newTreeSet(comparator);
        allTags = Sets.newHashSet();
        propertiesTypeByType = Maps.newHashMap();
        propertiesToIgnore = exportTags ? IGNORE : IGNORE_WITH_TAGS;
        addScopeColumn(attributesToExport, metaSchema);
    }

    private void addScopeColumn(ImmutableList<String> attributesToExport, MetaSchema metaSchema) {
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

    public void markAsVisited(String nodeTag, String keyAttribute, Node node) {
        allTags.add(node);
        SortedMap<String, String> properties = getKeyAttributeProperties(keyAttribute);
        for (Map.Entry<String, Object> property : node.getAllProperties().entrySet()) {
            String name = property.getKey();
            if (!name.startsWith("_") && !propertiesToIgnore.contains(name)) {
                String propertyType = getPropertyType(property.getValue());
                properties.put(name, propertyType);
            }
        }
    }

    public void add(Lineage lineage) {
        lineages.add(lineage);
    }

    public static String getPropertyType(Object value) {
        Class<?> clazz = value.getClass();
        if (clazz.isArray()) {
            return getSimpleType(clazz.getComponentType()) + "[]";
        } else {
            return getSimpleType(clazz);
        }
    }

    public static String getSimpleType(Class<?> clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return PropertyType.NUMBER.name();
        } else if (Boolean.class.isAssignableFrom(clazz)) {
            return PropertyType.BOOLEAN.name();
        } else {
            return PropertyType.STRING.name();
        }
    }

    private SortedMap<String, String> getKeyAttributeProperties(String keyAttribute) {
        return propertiesTypeByType.computeIfAbsent(keyAttribute, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
    }

}
