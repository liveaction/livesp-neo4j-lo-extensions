package com.livingobjects.neo4j;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.IOException;

public class LoadCSVExtensionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withExtension("/unmanaged", LoadCSVExtension.class);

    public static CloseableHttpClient httpClient;

    @BeforeClass
    public static void before() {
        httpClient = HttpClients.createDefault();
    }

    @AfterClass
    public static void after() {
        try {
            httpClient.close();
        } catch (IOException ignored) {
        }
    }


}