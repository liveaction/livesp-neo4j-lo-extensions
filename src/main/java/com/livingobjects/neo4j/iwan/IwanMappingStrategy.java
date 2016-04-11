package com.livingobjects.neo4j.iwan;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public final class IwanMappingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(IwanMappingStrategy.class);

    private static final String[] ACCEPTED_COLUMNS = new String[]{
            "c_client", "cluster_client_id", "cluster_client_name", "cluster_area_id", "cluster_area_name",
            "cluster_site_id", "cluster_site_name", "neType_cpe_id", "neType_cpe_name", "neType_cpe_ip",
            "neType_interface_id", "neType_interface_name", "neType_interface_ifIndex", "neType_interface_ifDescr",
            "cluster_client_tag", "cluster_area_tag", "cluster_site_tag", "neType_cpe_tag", "neType_interface_tag"};

    private final ImmutableMap<String, Integer> mapping;

    private IwanMappingStrategy(ImmutableMap<String, Integer> mapping) {
        this.mapping = mapping;
    }

    public static IwanMappingStrategy captureHeader(CSVReader reader) throws IOException {
        String[] headers = reader.readNext();
        ImmutableMap.Builder<String, Integer> mappingBldr = ImmutableMap.builder();

        LOGGER.warn("header : " + Arrays.toString(headers));

        int idx = 0;
        for (String header : headers) {
            mappingBldr.put(header, idx++);
        }

        ImmutableMap<String, Integer> mapping = mappingBldr.build();
        LOGGER.warn(Arrays.toString(mapping.keySet().toArray(new String[mapping.size()])));
        //TODO
//        if (mapping.size() != ACCEPTED_COLUMNS.length) {
//            throw new IllegalArgumentException("Invalid header for importing IWAN !");
//        }

        return new IwanMappingStrategy(mapping);
    }

    public int getColumnIndex(String name) {
        return mapping.get(name);
    }
}
