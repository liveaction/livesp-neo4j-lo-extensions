package com.livingobjects.neo4j;

import com.livingobjects.neo4j.model.Neo4jQuery;
import net.javacrumbs.jsonunit.fluent.JsonFluentAssert;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.assertj.core.util.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void shouldReturnASyntaxError() throws Exception {
        File csv = new File(getClass().getResource("/import.csv").toURI());

        Map<String, Object> params = Maps.newHashMap();
        params.put("testParam", "yes !");
        String statement = " LOAD CSV WITH HEADERS FROM {csvFile} AS csvLine\n" +
                " RETURN !!EXPECTED_SYNTAX_ERROR!!";
        Neo4jQuery query = new Neo4jQuery(statement, params);
        ObjectMapper JSON_MAPPER = new ObjectMapper();

        HttpEntity multipartBody = MultipartEntityBuilder.create().setContentType(ContentType.create("multipart/mixed"))
                .addTextBody("query", JSON_MAPPER.writeValueAsString(query), ContentType.APPLICATION_JSON)
                .addBinaryBody("file", csv)
                .build();

        HttpHost host = new HttpHost(neo4j.httpURI().getHost(), neo4j.httpURI().getPort());

        HttpPost request = new HttpPost("/unmanaged/load-csv");
        request.setEntity(multipartBody);

        try (CloseableHttpResponse response = httpClient.execute(host, request)) {

            assertThat(response.getStatusLine().getStatusCode()).isEqualTo(500);

            JsonFluentAssert
                    .assertThatJson(content(response))
                    .ignoring("%IGNORE%")
                    .isEqualTo("{\"error\":{\"code\":\"org.neo4j.graphdb.QueryExecutionException\",\"message\":\"%IGNORE%\"}}");
        }
    }


    @Test
    public void shouldLoadCSV() throws Exception {
        File csv = new File(getClass().getResource("/import.csv").toURI());

        Map<String, Object> params = Maps.newHashMap();
        params.put("testParam", "yes !");
        String statement = " LOAD CSV WITH HEADERS FROM {file} AS csvLine\n" +
                " WITH 'test_val_' + csvLine.col1 AS col1GeneratedValue, csvLine AS csvLine\n" +
                " CREATE (n:TestNode {col1:col1GeneratedValue})\n" +
                " SET n.col2 = csvLine.col2, n.col3 = {testParam}";
        Neo4jQuery query = new Neo4jQuery(statement, params);
        ObjectMapper JSON_MAPPER = new ObjectMapper();

        HttpEntity multipartBody = MultipartEntityBuilder.create().setContentType(ContentType.create("multipart/mixed"))
                .addTextBody("query", JSON_MAPPER.writeValueAsString(query), ContentType.APPLICATION_JSON)
                .addBinaryBody("file", csv)
                .build();

        HttpHost host = new HttpHost(neo4j.httpURI().getHost(), neo4j.httpURI().getPort());

        HttpPost request = new HttpPost("/unmanaged/load-csv");
        request.setEntity(multipartBody);

        try (CloseableHttpResponse response = httpClient.execute(host, request)) {

            assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

            JsonFluentAssert
                    .assertThatJson(content(response))
                    .ignoring("%IGNORE%")
                    .isEqualTo("{\"stats\":{\"nodes_deleted\":0,\"relationships_created\":0,\"relationships_deleted\":0,\"properties_set\":6,\"labels_added\":2,\"nodes_created\":2,\"labels_removed\":0,\"indexes_added\":0,\"indexes_removed\":0,\"constraints_added\":0,\"constraints_removed\":0,\"deleted_nodes\":0,\"deleted_relationships\":0}}");
        }
    }


    @Test
    public void shouldLoadCSVUsingPeriodicCommit() throws Exception {
        File csv = new File(getClass().getResource("/import.csv").toURI());

        Map<String, Object> params = Maps.newHashMap();
        params.put("testParam", "yes !");
        String statement = "USING PERIODIC COMMIT 1000\n" +
                " LOAD CSV WITH HEADERS FROM {file} AS csvLine\n" +
                " WITH 'test_val_' + csvLine.col1 AS col1GeneratedValue, csvLine AS csvLine\n" +
                " CREATE (n:TestNode {col1:col1GeneratedValue})\n" +
                " SET n.col2 = csvLine.col2, n.col3 = {testParam}";
        Neo4jQuery query = new Neo4jQuery(statement, params);
        ObjectMapper JSON_MAPPER = new ObjectMapper();

        HttpEntity multipartBody = MultipartEntityBuilder.create().setContentType(ContentType.create("multipart/mixed"))
                .addTextBody("query", JSON_MAPPER.writeValueAsString(query), ContentType.APPLICATION_JSON)
                .addBinaryBody("file", csv)
                .build();

        HttpHost host = new HttpHost(neo4j.httpURI().getHost(), neo4j.httpURI().getPort());

        HttpPost request = new HttpPost("/unmanaged/load-csv");
        request.setEntity(multipartBody);

        try (CloseableHttpResponse response = httpClient.execute(host, request)) {

            assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

            JsonFluentAssert
                    .assertThatJson(content(response))
                    .ignoring("%IGNORE%")
                    .isEqualTo("{\"stats\":{\"nodes_deleted\":0,\"relationships_created\":0,\"relationships_deleted\":0,\"properties_set\":6,\"labels_added\":2,\"nodes_created\":2,\"labels_removed\":0,\"indexes_added\":0,\"indexes_removed\":0,\"constraints_added\":0,\"constraints_removed\":0,\"deleted_nodes\":0,\"deleted_relationships\":0}}");
        }
    }


    @Test
    public void shouldLoadCSVAsync() throws Exception {
        File csv = new File(getClass().getResource("/import.csv").toURI());

        Map<String, Object> params = Maps.newHashMap();
        params.put("testParam", "yes !");
        String statement = " LOAD CSV WITH HEADERS FROM {file} AS csvLine\n" +
                " WITH 'test_val_' + csvLine.col1 AS col1GeneratedValue, csvLine AS csvLine\n" +
                " CREATE (n:TestNode {col1:col1GeneratedValue})\n" +
                " SET n.col2 = csvLine.col2, n.col3 = {testParam}";
        Neo4jQuery query = new Neo4jQuery(statement, params);
        ObjectMapper JSON_MAPPER = new ObjectMapper();

        HttpEntity multipartBody = MultipartEntityBuilder.create().setContentType(ContentType.create("multipart/mixed"))
                .addTextBody("query", JSON_MAPPER.writeValueAsString(query), ContentType.APPLICATION_JSON)
                .addBinaryBody("file", csv)
                .build();

        HttpHost host = new HttpHost(neo4j.httpURI().getHost(), neo4j.httpURI().getPort());

        HttpPost request = new HttpPost("/unmanaged/load-csv?async=true");
        request.setEntity(multipartBody);
        String uuid = null;
        try (CloseableHttpResponse response = httpClient.execute(host, request)) {

            assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

            uuid = content(response);
            assertThat(uuid).isNotEmpty();
        }

        try (CloseableHttpResponse response = httpClient.execute(host, new HttpGet("/unmanaged/load-csv/" + uuid))) {

            assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

            String content = content(response);

           /* JsonFluentAssert
                    .assertThatJson(content)
                    .ignoring("%IGNORE%")
                    .isEqualTo("{\"status\":\"%IGNORE%\"}");*/
        }
    }


    private String content(CloseableHttpResponse response) throws IOException {
        return IOUtils.toString(response.getEntity().getContent());
    }

}