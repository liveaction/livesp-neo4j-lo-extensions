package com.livingobjects.neo4j.model;

import com.livingobjects.neo4j.model.header.HeaderElement;
import com.livingobjects.neo4j.model.header.MultiElementHeader;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public final class HeaderElementTest {

    @Test
    @Parameters({
            "neType:interface.id, id, STRING, false, neType:interface",
            "neType:interface.id:NUMBER, id, NUMBER, false, neType:interface",
            "neType:interface.id:NUMBER[], id, NUMBER, true, neType:interface",
            "(neType:interface" + ELEMENT_SEPARATOR + "neType:cos).id:NUMBER, id, NUMBER, false, neType:interface, neType:cos",
    })
    public void shouldReadCsvHeader(
            String header, String expectedProperty, PropertyType expectedType, boolean expectedIsArray,
            String... expectedName) throws Exception {

        HeaderElement actual = HeaderElement.of(header, 1);

        assertThat(actual.elementName).isEqualTo(expectedName[0]);
        if (expectedName.length > 1) {
            assertThat(actual).isInstanceOf(MultiElementHeader.class);
            Assertions.assertThat(((MultiElementHeader) actual).targetElementName).isEqualTo(expectedName[1]);
        }
        assertThat(actual.propertyName).isEqualTo(expectedProperty);
        assertThat(actual.type).isEqualTo(expectedType);
        assertThat(actual.isArray).isEqualTo(expectedIsArray);
    }
}