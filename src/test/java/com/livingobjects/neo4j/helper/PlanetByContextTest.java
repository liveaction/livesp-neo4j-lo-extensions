package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.exception.InsufficientContextException;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PlanetByContextTest {

    @Test
    public void should_not_match_if_no_attributes_match() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:*")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class).hasMessage("No planet found");
        }
    }

    @Test
    @Ignore
    public void should_not_match_if_no_attributes_match_event_if_it_is_the_only_one() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe"))
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class).hasMessage("No planet found");
        }
    }

    @Test
    public void should_not_match_if_no_attribute_values_match() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:*")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        try {
            planetByContext.bestMatchingContext(ImmutableSet.of(
                    "neType:wanLink",
                    "bandwidth:1000",
                    "loopback:172.17.10.22:5000"
            ));
            fail("Should fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(InsufficientContextException.class).hasMessage("No planet found");
        }
    }

    @Test
    public void should_match_attribute_and_less_difference() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:*")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe");
    }

    @Test
    public void should_match_attribute_and_less_difference_no_matters_the_order() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:*")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe");
    }

    @Test
    @Ignore
    public void should_match_attribute_type_VS_no_attribute() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:*")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "vendor:one_access",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe/*");
    }

    @Test
    public void should_match_attribute_value_VS_no_attribute() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "vendor:cisco",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe/cisco");
    }

    @Test
    public void should_match_attribute_value_VS_attribute_type() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:*")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "neType:cpe",
                "vendor:huawei",
                "bandwidth:1000",
                "loopback:172.17.10.22:5000"
        ))).isEqualTo("iwan/{:scopeId}/cpe/huawei");
    }

    @Test
    public void should_returns_the_fallback_planet_event_in_different_order() throws Exception {
        PlanetByContext planetByContext = new PlanetByContext(ImmutableSet.of(
                Maps.immutableEntry("iwan/{:scopeId}/cpe/cisco", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:cisco")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/*", ImmutableSet.of("domain:iwan", "neType:cpe")),
                Maps.immutableEntry("iwan/{:scopeId}/cpe/huawei", ImmutableSet.of("domain:iwan", "neType:cpe", "vendor:huawei"))
        ));
        Assertions.assertThat(planetByContext.bestMatchingContext(ImmutableSet.of(
                "domain:iwan",
                "neType:cpe",
                "vendor:unmapped_vendor"
        ))).isEqualTo("iwan/{:scopeId}/cpe/*");
    }

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