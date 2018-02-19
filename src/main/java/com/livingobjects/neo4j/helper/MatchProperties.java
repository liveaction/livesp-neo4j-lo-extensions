package com.livingobjects.neo4j.helper;

public final class MatchProperties {
    public final String key1;
    public final Object value1;
    public final String key2;
    public final Object value2;

    public static MatchProperties of(String key1, Object value1) {
        return new MatchProperties(key1, value1, null, null);
    }

    public static MatchProperties of(String key1, Object value1, String key2, Object value2) {
        return new MatchProperties(key1, value1, key2, value2);
    }

    private MatchProperties(String key1, Object value1, String key2, Object value2) {
        this.key1 = key1;
        this.value1 = value1;
        this.key2 = key2;
        this.value2 = value2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchProperties that = (MatchProperties) o;

        if (key1 != null ? !key1.equals(that.key1) : that.key1 != null) return false;
        if (value1 != null ? !value1.equals(that.value1) : that.value1 != null) return false;
        if (key2 != null ? !key2.equals(that.key2) : that.key2 != null) return false;
        return value2 != null ? value2.equals(that.value2) : that.value2 == null;
    }

    @Override
    public int hashCode() {
        int result = key1 != null ? key1.hashCode() : 0;
        result = 31 * result + (value1 != null ? value1.hashCode() : 0);
        result = 31 * result + (key2 != null ? key2.hashCode() : 0);
        result = 31 * result + (value2 != null ? value2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        if (key2 != null) {
            return key1 + ':' + value1 + ", " + key2 + ':' + value2;
        } else {
            return key1 + ':' + value1;
        }
    }
}