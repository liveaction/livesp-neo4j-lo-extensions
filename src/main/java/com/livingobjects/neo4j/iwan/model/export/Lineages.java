package com.livingobjects.neo4j.iwan.model.export;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.HeaderElement;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.TAG;

public final class Lineages {

    private static final Set<String> IGNORE = ImmutableSet.of("createdAt", "updatedAt", "createdBy", "updatedBy");

    public final Set<Lineage> lineages;

    public final Set<String> allTags;

    public final Map<String, SortedMap<String, String>> propertiesTypeByType;

    public Lineages(ImmutableList<String> attributesToExport) {
        lineages = Sets.newTreeSet(new LineageComparator(attributesToExport));
        allTags = Sets.newHashSet();
        propertiesTypeByType = Maps.newHashMap();
    }

    public boolean dejaVu(Node leaf) {
        return allTags.contains(leaf.getProperty(TAG).toString());
    }

    public void markAsVisited(String nodeTag, String type, Node node) {
        allTags.add(nodeTag);
        SortedMap<String, String> properties = propertiesTypeByType.computeIfAbsent(type, k -> Maps.newTreeMap(PropertyNameComparator.PROPERTY_NAME_COMPARATOR));
        for (Map.Entry<String, Object> property : node.getAllProperties().entrySet()) {
            String name = property.getKey();
            if (!name.startsWith("_") && !IGNORE.contains(name)) {
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
            return HeaderElement.Type.NUMBER.name();
        } else if (Boolean.class.isAssignableFrom(clazz)) {
            return HeaderElement.Type.BOOLEAN.name();
        } else {
            return HeaderElement.Type.STRING.name();
        }
    }

}
