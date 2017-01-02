package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class StringTemplateTest {

    private static final ImmutableMap<String, Integer> HEADER = ImmutableMap.of("client.id", 0, "client.tag", 1);
    private static final String[] LINE = {"00001", "tag=client,client=tag00001"};

    @Test
    public void shouldNotModifyEmptyString() {
        String result = StringTemplate.template("", HEADER, LINE);
        assertThat(result).isEqualTo("");
    }

    @Test
    public void shouldNotModifyNonTemplateString() {
        String result = StringTemplate.template("Je suis chien méchant, elle me fait manger", HEADER, LINE);
        assertThat(result).isEqualTo("Je suis chien méchant, elle me fait manger");
    }

    @Test
    public void shouldApplyTemplateOnASimpleString() {
        String result = StringTemplate.template("Je suis chien méchant, elle me fait manger dans sa ${client.id}", HEADER, LINE);
        assertThat(result).isEqualTo("Je suis chien méchant, elle me fait manger dans sa 00001");
    }

    @Test
    public void shouldApplyTemplateOnAComplexString() {
        String result = StringTemplate.template("Je suis chien méchant, elle me fait manger dans sa ${client.id}. Blabla ${client.id}. Blip blop ${client.tag}", HEADER, LINE);
        assertThat(result).isEqualTo("Je suis chien méchant, elle me fait manger dans sa 00001. Blabla 00001. Blip blop tag=client,client=tag00001");
    }

    @Test
    public void shouldApplyTemplateOnASuperComplexString() {
        try {
            StringTemplate.template("Je suis chien méchant, elle me fait ${non.existant} manger dans sa ${client.id}. Blabla ${client.id}. Blip blop ${client.tag}", HEADER, LINE);
            fail("should thorw an IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("Unable to replace variable ${non.existant} in string 'Je suis chien méchant, elle me fait ${non.existant} manger dans sa ${client.id}. Blabla ${client.id}. Blip blop ${client.tag}'.");
        }
    }


}