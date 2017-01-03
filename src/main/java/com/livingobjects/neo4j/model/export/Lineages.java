package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.PropertyType;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import org.neo4j.graphdb.Node;

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

    public final Set<String> allTags;

    public final Map<String, SortedMap<String, String>> propertiesTypeByType;

    private final Set<String> propertiesToIgnore;

    public Lineages(ImmutableList<String> attributesToExport, boolean exportTags) {
        lineages = Sets.newTreeSet(new LineageComparator(attributesToExport));
        allTags = Sets.newHashSet();
        propertiesTypeByType = Maps.newHashMap();
        propertiesToIgnore = exportTags ? IGNORE : IGNORE_WITH_TAGS;
    }

    public boolean dejaVu(Node leaf) {
        return allTags.contains(leaf.getProperty(IwanModelConstants.TAG).toString());
    }

    public void markAsVisited(String nodeTag, String type, Node node) {
        allTags.add(nodeTag);
        SortedMap<String, String> properties = propertiesTypeByType.computeIfAbsent(type, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
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

}
