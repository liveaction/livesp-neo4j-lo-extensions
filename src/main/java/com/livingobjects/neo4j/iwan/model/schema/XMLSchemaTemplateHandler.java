package com.livingobjects.neo4j.iwan.model.schema;

import com.livingobjects.neo4j.iwan.model.schema.model.SchemaTemplate;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public final class XMLSchemaTemplateHandler extends DefaultHandler {


    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        switch (qName) {
            case "protocol":
                break;
        }
    }

    public SchemaTemplate getTemplate() {
        return new SchemaTemplate(null, null, null, null);
    }
}
