package com.livingobjects.neo4j.helper;

import com.livingobjects.neo4j.model.PropertyType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public final class PropertyConverterTest {

    @Test
    public void shouldParseDouble() {
        String point1 = "0.1";

        double point1Converted = (double) PropertyConverter.convert(point1, PropertyType.NUMBER, false);

        assertThat(point1Converted).isEqualTo(0.1);
    }

    @Test
    public void shouldParseDoubleArray() {
        String pointNumberArray = "[0.1, 0.2, 0.3]";

        double[] pointNumberArrayConverted = (double[]) PropertyConverter.convert(pointNumberArray, PropertyType.NUMBER, true);

        assertThat(pointNumberArrayConverted).containsOnly(0.1, 0.2, 0.3);
    }

    @Test
    public void shouldParsePointNumberAsDouble() {
        String point1 = ".1";

        double point1Converted = (double) PropertyConverter.convert(point1, PropertyType.NUMBER, false);

        assertThat(point1Converted).isEqualTo(.1);
    }

    @Test
    public void shouldParsePointNumberArrayAsDoubleArray() {
        String pointNumberArray = "[.1, .2, .3]";

        double[] pointNumberArrayConverted = (double[]) PropertyConverter.convert(pointNumberArray, PropertyType.NUMBER, true);

        assertThat(pointNumberArrayConverted).containsOnly(.1, .2, .3);
    }

    @Test
    public void should_parse_string_array() {
        String stringArray = "[\".1 , hey ho !\", \".2\", \".3\"]";

        String[] actual = (String[]) PropertyConverter.convert(stringArray, PropertyType.STRING, true);

        assertThat(actual).containsOnly(".1 , hey ho !", ".2", ".3");
    }
}