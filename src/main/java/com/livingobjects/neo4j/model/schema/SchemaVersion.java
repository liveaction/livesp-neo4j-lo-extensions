package com.livingobjects.neo4j.model.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class SchemaVersion implements Comparable<SchemaVersion> {

    public static final Comparator<SchemaVersion> COMPARATOR = (version1, version2) -> {
        if (version1.components.equals(version2.components)) return 0;

        List<Integer> vals1 = version1.components;
        List<Integer> vals2 = version2.components;

        int i = 0;
        while (i < vals1.size() && i < vals2.size() && vals1.get(i).equals(vals2.get(i))) i++;

        if (i < vals1.size() && i < vals2.size())
            return vals1.get(i).compareTo(vals2.get(i));

        if (i < vals1.size()) {
            boolean allZeros = true;
            for (int j = i; allZeros & (j < vals1.size()); j++)
                allZeros = (vals1.get(j).equals(0));
            return allZeros ? 0 : 1;
        }

        if (i < vals2.size()) {
            boolean allZeros = true;
            for (int j = i; allZeros & (j < vals2.size()); j++)
                allZeros = (vals2.get(j).equals(0));
            return allZeros ? 0 : -1;
        }

        return 0;
    };

    public List<Integer> components;

    public static SchemaVersion of(Integer... components) {
        return new SchemaVersion(Arrays.asList(components));
    }

    public static SchemaVersion of(String components) {
        List<Integer> builder = new ArrayList<>();
        String[] split = components.split("\\.");
        for (String s : split) {
            builder.add(Integer.valueOf(s.trim()));
        }
        return new SchemaVersion(builder);
    }

    public SchemaVersion() {
    }

    public SchemaVersion(List<Integer> components) {
        this.components = components;
    }

    public List<Integer> getComponents() {
        return components;
    }

    public void setComponents(List<Integer> components) {
        this.components = components;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaVersion that = (SchemaVersion) o;
        return Objects.equals(components, that.components);
    }

    @Override
    public int hashCode() {
        return Objects.hash(components);
    }

    @Override
    public int compareTo(SchemaVersion o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Integer component : components) {
            if (builder.length() > 0) {
                builder.append('.');
            }
            builder.append(component);
        }
        return builder.toString();
    }
}