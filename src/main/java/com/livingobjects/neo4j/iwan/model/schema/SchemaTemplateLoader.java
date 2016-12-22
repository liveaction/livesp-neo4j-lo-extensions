package com.livingobjects.neo4j.iwan.model.schema;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.iwan.model.exception.SchemaTemplateException;
import com.livingobjects.neo4j.iwan.model.schema.factories.CustomNodeFactory;
import com.livingobjects.neo4j.iwan.model.schema.model.Node;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaTemplate;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public final class SchemaTemplateLoader {

    private final Map<NodeType, CustomNodeFactory> nodeFactories = Maps.newHashMap();

    public void loadAndApplyTemplate(InputStream csv, InputStream xmlTemplate) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(csv));
        ImmutableMap<String, Integer> header = readCsvHeader(reader);

        SchemaTemplate template = parseTemplate(xmlTemplate);

        String[] line = reader.readNext();
        while (line != null) {

            applyTemplate(template, line);

            line = reader.readNext();
        }

    }

    private void applyTemplate(SchemaTemplate template, String[] line) {
        template.templateNode
    }

    private SchemaTemplate parseTemplate(InputStream xmlTemplate) throws SchemaTemplateException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser saxParser = factory.newSAXParser();
            XMLSchemaTemplateHandler handler = new XMLSchemaTemplateHandler();
            saxParser.parse(xmlTemplate, handler);
            return handler.getTemplate();
        } catch (ParserConfigurationException | SAXException e) {
            throw new SchemaTemplateException("Unable to instanciate XML parser", e);
        } catch (IOException e) {
            throw new SchemaTemplateException("Unable to parse XML template", e);
        }
    }

    private ImmutableMap<String, Integer> readCsvHeader(CSVReader reader) throws IOException {
        ImmutableMap.Builder<String, Integer> headerBuilder = ImmutableMap.builder();
        String[] headerLine = reader.readNext();
        if (headerLine != null) {
            for (int index = 0; index < headerLine.length; index++) {
                String column = headerLine[index];
                headerBuilder.put(column, index);
            }
        }
        return headerBuilder.build();
    }

    private CustomNodeFactory nodeFactory(Node node) {
        new NodeType(node.labels, node.keys.keySet());
    }

    private final class NodeType {
        public final ImmutableList<String> labels;
        public final ImmutableList<String> keys;

        public NodeType(ImmutableSet<String> labels, ImmutableSet<String> strings) {

        }
    }

}
