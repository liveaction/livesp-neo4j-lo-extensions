package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class PlanetByContextTest {

    @Test
    public void should_returns_single_attributes() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"))
        ));
        Assertions.assertThat(planetByContext.distinctAttributes()).containsExactly();
    }

    @Test
    public void should_returns_distinct_attributes() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.distinctAttributes()).containsExactly("vendor:cisco", "vendor:huawei");
    }

}