package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.exception.InsufficientContextException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class PlanetByContextTest {

    @Test
    public void should_not_match_if_no_attributes_match() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class).hasMessage("No PlanetTemplate eligible for this context !");
            assertThat(((InsufficientContextException) e).missingAttributesToChoose).containsExactly("domain:iwan", "neType:cpe", "vendor:cisco", "vendor:huawei");
        }
    }

    @Test
    public void should_not_match_if_no_attributes_match_event_if_it_is_the_only_one() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class).hasMessage("No PlanetTemplate eligible for this context !");
            assertThat(((InsufficientContextException) e).missingAttributesToChoose).containsExactly("domain:iwan", "neType:cpe");
        }
    }

    @Test
    public void should_not_match_if_no_attribute_values_match() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "neType:wanLink",
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class).hasMessage("No PlanetTemplate eligible for this context !");
        }
    }

    @Test
    public void should_fail_if_cannot_choose() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpevip", ImmutableSet.of("domain:viptela", "neType:cpe"),
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "neType:cpe",
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class)
                    .hasMessage("Multiple PlanetTemplate eligible but no sufficient context to choose certainty !");
            assertThat(((InsufficientContextException) e).missingAttributesToChoose).containsExactly("domain:viptela", "domain:iwan");
        }
    }

    @Test
    public void should_match_attribute_and_less_difference() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe");
    }

    @Test
    public void should_match_attribute_and_less_difference_no_matters_the_order() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe");
    }

    @Test
    public void should_match_attribute_type_VS_no_attribute() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "vendor:one_access",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe");
    }

    @Test
    public void should_match_attribute_value_VS_no_attribute() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "vendor:cisco",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe/cisco");
    }

    @Test
    public void should_match_attribute_value_VS_attribute_type() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"),
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "vendor:huawei",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe/huawei");
    }

    @Test
    public void should_returns_the_fallback_planet_even_in_different_order() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"),
                "iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")
        ));
        assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "domain:iwan",
                "neType:cpe",
                "vendor:unmapped_vendor"
        ))).isEqualTo("iwan/{:scopeId}/cpe");
    }

    @Test
    public void should_returns_single_attributes() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")
        ));
        assertThat(planetByContext.distinctAttributes(ImmutableSet.of("iwan/{:scopeId}/cpe/cisco"))).containsExactly();
    }

    @Test
    public void should_returns_distinct_attributes() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableMap.of(
                "iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco"),
                "iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei")
        ));
        assertThat(planetByContext.distinctAttributes(ImmutableSet.of("iwan/{:scopeId}/cpe/cisco", "iwan/{:scopeId}/cpe/huawei"))).containsExactly("vendor:cisco", "vendor:huawei");
    }

}