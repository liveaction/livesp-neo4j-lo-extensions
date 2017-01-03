package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringTemplate {

    public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]*)}");

    public static String template(String value, ImmutableMap<String, Integer> header, String[] line) {
        int varCharIndex = value.indexOf('$');
        if (varCharIndex == -1) {
            return value;
        } else {
            StringBuffer buffer = new StringBuffer();
            Matcher matcher = VARIABLE_PATTERN.matcher(value);
            while (matcher.find()) {
                String group = matcher.group(1);
                if (group != null) {
                    Integer headerIndex = header.get(group);
                    if (headerIndex != null) {
                        String replacement = line[headerIndex];
                        matcher.appendReplacement(buffer, replacement);
                    } else {
                        throw new IllegalStateException("Unable to replace variable ${" + group + "} in string '" + value + "'.");
                    }
                }
            }
            matcher.appendTail(buffer);
            return buffer.toString();
        }
    }

}
