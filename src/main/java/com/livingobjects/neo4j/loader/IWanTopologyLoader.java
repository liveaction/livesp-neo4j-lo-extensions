package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.model.exception.ImportException;
import com.livingobjects.neo4j.model.exception.InvalidSchemaException;
import com.livingobjects.neo4j.model.exception.InvalidScopeException;
import com.livingobjects.neo4j.model.header.HeaderElement;
import com.livingobjects.neo4j.model.header.HeaderElement.Visitor;
import com.livingobjects.neo4j.model.header.MultiElementHeader;
import com.livingobjects.neo4j.model.header.SimpleElementHeader;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.result.Neo4jLoadResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;

public final class IWanTopologyLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(IWanTopologyLoader.class);
    private static final int MAX_TRANSACTION_COUNT = 500;

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final UniqueElementFactory networkElementFactory;

    private final ImmutableMap<String, Node> attributeNodes;
    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    private final ImmutableMap<String, ImmutableSet<String>> crossAttributesRelations;
    private final ImmutableList<String> scopes;
    private ImmutableMap<String, Set<String>> lineage;

    private final Map<String, ImmutableMultimap<String, Node>> planetsByClient = Maps.newHashMap();
    private final Set<Long> planetLinksCreatedForNodes = Sets.newHashSet();

    public IWanTopologyLoader(GraphDatabaseService graphDb, MetricRegistry metrics) {
        this.metrics = metrics;
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {

            this.networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);

            ImmutableMap.Builder<String, Node> attributesBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableSet<String>> crossAttributesRelationsBldr = ImmutableMap.builder();
            ImmutableList.Builder<String> scopesBldr = ImmutableList.builder();
            graphDb.findNodes(IwanModelConstants.LABEL_ATTRIBUTE).forEachRemaining(n -> {
                String keytype = n.getProperty(IwanModelConstants._TYPE).toString();
                String key = keytype + IwanModelConstants.KEYTYPE_SEPARATOR + n.getProperty(IwanModelConstants.NAME).toString();
                attributesBldr.put(key, n);
                if (IwanModelConstants.KEY_TYPES.contains(keytype)) {
                    ImmutableList.Builder<Relationship> crels = ImmutableList.builder();
                    ImmutableList.Builder<Relationship> prels = ImmutableList.builder();
                    ImmutableSet.Builder<String> crossRels = ImmutableSet.builder();
                    n.getRelationships(Direction.INCOMING, IwanModelConstants.LINK_PARENT).forEach(crels::add);
                    n.getRelationships(Direction.OUTGOING, IwanModelConstants.LINK_PARENT).forEach(prels::add);
                    n.getRelationships(Direction.OUTGOING, IwanModelConstants.LINK_CROSS_ATTRIBUTE).forEach(r -> {
                        Node endNode = r.getEndNode();
                        if (endNode.hasLabel(IwanModelConstants.LABEL_ATTRIBUTE)) {
                            Object typeProperty = endNode.getProperty(IwanModelConstants._TYPE);
                            Object nameProperty = endNode.getProperty(IwanModelConstants.NAME);
                            if (typeProperty != null && nameProperty != null) {
                                String endKeytype = typeProperty.toString() + IwanModelConstants.KEYTYPE_SEPARATOR + nameProperty.toString();
                                crossRels.add(endKeytype);
                            }
                        }
                    });
                    if (prels.build().isEmpty()) {
                        scopesBldr.add(key);
                    }
                    childrenRelationsBldr.put(key, crels.build());
                    parentRelationsBldr.put(key, prels.build());
                    crossAttributesRelationsBldr.put(key, crossRels.build());
                }
            });

            this.attributeNodes = attributesBldr.build();
            this.childrenRelations = childrenRelationsBldr.build();
            this.parentRelations = parentRelationsBldr.build();
            this.crossAttributesRelations = crossAttributesRelationsBldr.build();
            this.scopes = scopesBldr.build();
        }
    }

    public Neo4jLoadResult loadFromStream(InputStream is) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(is));
        IwanMappingStrategy strategy = IwanMappingStrategy.captureHeader(reader);

        checkCrossAttributeDefinitionExists(strategy);

        String[] nextLine;

        int lineIndex = 0;
        int imported = 0;
        List<String[]> currentTransaction = Lists.newArrayListWithCapacity(MAX_TRANSACTION_COUNT);
        Map<Integer, String> errors = Maps.newHashMap();

        Transaction tx = graphDb.beginTx();
        lineage = strategy.guessElementCreationStrategy(scopes, childrenRelations);
        ImmutableSet<String> startKeytypes = ImmutableSet.copyOf(
                scopes.stream().filter(strategy::hasKeyType).collect(Collectors.toSet()));

        while ((nextLine = reader.readNext()) != null) {
            try {
                importLine(nextLine, startKeytypes, strategy);
                imported++;
                currentTransaction.add(nextLine);
                if (currentTransaction.size() >= MAX_TRANSACTION_COUNT) {
                    tx = renewTransaction(tx);
                    currentTransaction.clear();
                }
            } catch (ImportException | NoSuchElementException e) {
                tx = properlyRenewTransaction(strategy, currentTransaction, tx, startKeytypes);
                errors.put(lineIndex, e.getMessage());
                LOGGER.warn(e.getLocalizedMessage());
                LOGGER.debug("STACKTRACE", e);
                LOGGER.debug(Arrays.toString(nextLine));
            } catch (IllegalArgumentException e) {
                tx = properlyRenewTransaction(strategy, currentTransaction, tx, startKeytypes);
                errors.put(lineIndex, e.getMessage());
                LOGGER.error(e.getLocalizedMessage());
                LOGGER.error("STACKTRACE", e);
                LOGGER.debug(Arrays.toString(nextLine));
            }
            lineIndex++;
        }
        tx.success();
        tx.close();

        return new Neo4jLoadResult(imported, errors);
    }

    private Transaction properlyRenewTransaction(IwanMappingStrategy strategy, List<String[]> currentTransaction, Transaction tx, ImmutableSet<String> startKeytypes) {
        tx = renewTransaction(tx, true);
        tx = reloadValidTransactionLines(tx, currentTransaction, startKeytypes, strategy);
        currentTransaction.clear();
        return tx;
    }

    private Transaction renewTransaction(Transaction tx) {
        return renewTransaction(tx, false);
    }

    private Transaction renewTransaction(Transaction tx, boolean asFailure) {
        if (asFailure) {
            tx.failure();
        } else {
            tx.success();
        }
        tx.close();
        return graphDb.beginTx();
    }

    private Transaction reloadValidTransactionLines(
            Transaction tx, List<String[]> lines, ImmutableSet<String> startKeytypes, IwanMappingStrategy strategy) {
        if (!lines.isEmpty()) {
            lines.forEach(ct -> importLine(ct, startKeytypes, strategy));
            return renewTransaction(tx);
        }
        return tx;
    }

    private void importLine(String[] line, ImmutableSet<String> startKeytypes, IwanMappingStrategy strategy) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {

            // Try to update elements which is not possible to create (no parent founds)
            // updated elements must exist or NoSuchElement was throw
            ImmutableMap.Builder<String, Optional<UniqueEntity<Node>>> allNodesBldr = ImmutableMap.builder();
            strategy.getAllElementsType().stream()
                    .filter(key -> !lineage.keySet().contains(key))
                    .forEach(key -> allNodesBldr.put(key, updateElement(strategy, line, key)));

            // Create elements
            Map<String, Optional<UniqueEntity<Node>>> nodes = lineage.keySet().stream()
                    .map(key -> Maps.immutableEntry(key, createElement(strategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            ImmutableMap<String, Optional<UniqueEntity<Node>>> allNodes = allNodesBldr.putAll(nodes).build();

            createCrossAttributeLinks(line, strategy, allNodes);

            createConnectLink(startKeytypes, nodes);

            createPlanetLink(startKeytypes, nodes);

            ImmutableMultimap<String, Node> planets;
            if (startKeytypes.isEmpty()) {
                planets = getGlobalPlanets();
                for (Entry<String, Optional<UniqueEntity<Node>>> entry : nodes.entrySet()) {
                    ImmutableCollection<Node> parents = planets.get(entry.getKey());
                    entry.getValue().ifPresent(nodeEntity -> {
                        if (nodeEntity.wasCreated) {
                            Node node = nodeEntity.entity;
                            long id = node.getId();
                            if (!planetLinksCreatedForNodes.contains(id)) {
                                createOutgoingUniqueLinks(node, parents, IwanModelConstants.LINK_ATTRIBUTE);
                                planetLinksCreatedForNodes.add(id);
                            }
                        }
                    });
                }
            }
        }
    }

    private void createCrossAttributeLinks(String[] line, IwanMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createCrossAttributeLinks").time()) {
            Map<String, Relationship> crossAttributeRelationships = createCrossAttributeRelationships(nodes);
            for (MultiElementHeader meHeader : strategy.getMultiElementHeader()) {
                String key = meHeader.elementName + ELEMENT_SEPARATOR + meHeader.targetElementName;
                Optional<UniqueEntity<Node>> fromNode = nodes.get(meHeader.elementName);
                Optional<UniqueEntity<Node>> toNode = nodes.get(meHeader.targetElementName);
                if (fromNode.isPresent() && toNode.isPresent()) {
                    Relationship relationship = crossAttributeRelationships.get(key);
                    if (relationship != null) {
                        persistElementProperty(meHeader, line, relationship);
                    }
                }
            }
        }
    }

    private void checkCrossAttributeDefinitionExists(IwanMappingStrategy strategy) throws InvalidSchemaException {
        for (MultiElementHeader meHeader : strategy.getMultiElementHeader()) {
            ImmutableSet<String> crossAttributesLinks = crossAttributesRelations.get(meHeader.elementName);
            if (crossAttributesLinks == null || !crossAttributesLinks.contains(meHeader.targetElementName)) {
                throw new InvalidSchemaException("Schema does not allow to create cross attribute between '" + meHeader.elementName + "' and '" + meHeader.targetElementName + "'. Import aborted.");
            }
        }
    }

    private Map<String, Relationship> createCrossAttributeRelationships(Map<String, Optional<UniqueEntity<Node>>> nodes) {
        Map<String, Relationship> multiElementLinks = Maps.newHashMap();
        for (Entry<String, ImmutableSet<String>> rel : crossAttributesRelations.entrySet()) {
            String keyType = rel.getKey();
            Optional<UniqueEntity<Node>> startNode = nodes.getOrDefault(keyType, Optional.empty());
            startNode.ifPresent(fromNode -> {
                for (String endKeyType : rel.getValue()) {
                    Optional<UniqueEntity<Node>> optEndNode = nodes.getOrDefault(endKeyType, Optional.empty());
                    optEndNode.ifPresent(toNode -> {
                        Relationship link = createOutgoingUniqueLink(fromNode.entity, toNode.entity, IwanModelConstants.LINK_CROSS_ATTRIBUTE);
                        String key = keyType + ELEMENT_SEPARATOR + endKeyType;
                        multiElementLinks.put(key, link);
                    });
                }
            });
        }
        return multiElementLinks;
    }

    private void createConnectLink(ImmutableSet<String> startKeytypes, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        for (String keytype : nodes.keySet()) {
            nodes.get(keytype).ifPresent(node -> {
                int relCount = linkToParents(keytype, node, nodes);
                if (!startKeytypes.isEmpty() && !scopes.contains(keytype) && relCount <= 0) {
                    throw new IllegalStateException("No parent element found for type " + keytype);
                }
            });
        }
    }

    private void createPlanetLink(ImmutableSet<String> startKeytypes, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        ImmutableMultimap<String, Node> planets;
        for (String keytype : startKeytypes) {
            UniqueEntity<Node> scopeNode = nodes.get(keytype).orElseThrow(() -> new InvalidScopeException("No scope tag provided."));
            Object scopeNodeIdProperty = scopeNode.entity.getProperty("id");
            if (scopeNodeIdProperty != null) {
                String id = scopeNodeIdProperty.toString();
                String name = keytype.substring(keytype.indexOf(IwanModelConstants.KEYTYPE_SEPARATOR) + 1);
                planets = getPlanetsForClient(name + IwanModelConstants.KEYTYPE_SEPARATOR + id);

                for (Entry<String, Optional<UniqueEntity<Node>>> entry : nodes.entrySet()) {
                    ImmutableCollection<Node> parents = planets.get(entry.getKey());
                    entry.getValue().ifPresent(node -> createOutgoingUniqueLinks(node.entity, parents, IwanModelConstants.LINK_ATTRIBUTE));
                }
            } else {
                throw new InvalidScopeException("No scope id provided.");
            }
        }
    }

    private int linkToParents(String keyType, UniqueEntity<Node> keyTypeNode, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-linkToParents").time()) {
            ImmutableList<Relationship> relationships = parentRelations.get(keyType);
            if (relationships == null || relationships.isEmpty()) {
                return -1;
            }

            int relCount = 0;
            for (Relationship relationship : relationships) {
                String toKeytype = relationship.getEndNode().getProperty(IwanModelConstants._TYPE).toString() + IwanModelConstants.KEYTYPE_SEPARATOR +
                        relationship.getEndNode().getProperty(IwanModelConstants.NAME).toString();
                Optional<UniqueEntity<Node>> parent = nodes.get(toKeytype);
                if (parent == null || !parent.isPresent()) {
                    continue;
                }
                ++relCount;
                createOutgoingUniqueLink(keyTypeNode.entity, parent.get().entity, IwanModelConstants.LINK_CONNECT);
            }

            return relCount;
        }
    }

    private Relationship createOutgoingUniqueLink(Node node, Node parent, RelationshipType linkType) {
        for (Relationship next : node.getRelationships(Direction.OUTGOING, linkType)) {
            if (next.getEndNode().equals(parent)) {
                return next;
            }
        }
        return node.createRelationshipTo(parent, linkType);
    }

    private void createOutgoingUniqueLinks(Node node, ImmutableCollection<Node> parents, RelationshipType linkType) {
        if (!parents.isEmpty()) {
            Set<Node> relationshipsToCreate = Sets.newHashSet(parents);
            for (Relationship next : node.getRelationships(Direction.OUTGOING, linkType)) {
                Node endNode = next.getEndNode();
                relationshipsToCreate.remove(endNode);
            }
            relationshipsToCreate.forEach(parent -> node.createRelationshipTo(parent, linkType));
        }
    }

    private Optional<UniqueEntity<Node>> createElement(IwanMappingStrategy strategy, String[] line, String elementName) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createElement").time()) {
            if (IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(elementName)) {
                return Optional.of(UniqueEntity.existing(graphDb.findNode(IwanModelConstants.LABEL_SCOPE, "tag", IwanModelConstants.SCOPE_GLOBAL_TAG)));
            }

            Node elementAttNode = attributeNodes.get(elementName);
            if (elementAttNode == null) {
                throw new IllegalStateException("Unknown element type: '" + elementName + "'");
            }
            boolean isOverridable = Boolean.parseBoolean(elementAttNode.getProperty(IwanModelConstants._OVERRIDABLE, Boolean.FALSE).toString());

            Set<String> todelete = parentRelations.get(elementName).stream()
                    .filter(r -> !IwanModelConstants.CARDINALITY_MULTIPLE.equals(r.getProperty(IwanModelConstants.CARDINALITY, "")))
                    .map(r -> r.getEndNode().getProperty(IwanModelConstants._TYPE).toString() + IwanModelConstants.KEYTYPE_SEPARATOR + r.getEndNode().getProperty(IwanModelConstants.NAME).toString())
                    .filter(strategy::hasKeyType)
                    .collect(Collectors.toSet());

            ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementName);
            HeaderElement tagHeader = elementHeaders.stream()
                    .filter(h -> IwanModelConstants.TAG.equals(h.propertyName))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException(IwanModelConstants.TAG + " not found for element " + elementName));

            String tag = line[tagHeader.index];

            if (!tag.isEmpty()) {
                UniqueEntity<Node> uniqueEntity = networkElementFactory.getOrCreateWithOutcome(IwanModelConstants.TAG, tag);
                Node elementNode = uniqueEntity.entity;

                if (uniqueEntity.wasCreated) {
                    if (scopes.contains(elementName)) {
                        elementNode.addLabel(IwanModelConstants.LABEL_SCOPE);
                    }
                    elementNode.setProperty(IwanModelConstants._TYPE, elementName);
                    if (isOverridable) {
                        elementNode.setProperty(IwanModelConstants._SCOPE, IwanModelConstants.SCOPE_GLOBAL_TAG);
                    }
                } else {
                    elementNode.setProperty(IwanModelConstants.UPDATED_AT, Instant.now().toEpochMilli());

                    elementNode.getRelationships(Direction.OUTGOING, IwanModelConstants.LINK_CONNECT).forEach(r -> {
                        String type = r.getEndNode().getProperty(IwanModelConstants._TYPE, IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE).toString();
                        if (todelete.contains(type)) {
                            r.delete();
                        }
                    });
                }

                persistElementProperties(line, elementHeaders, elementNode);

                return Optional.of(uniqueEntity);
            } else {
                return Optional.empty();
            }
        }
    }

    private Optional<UniqueEntity<Node>> updateElement(IwanMappingStrategy strategy, String[] line, String elementName) throws NoSuchElementException {
        ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementName);
        HeaderElement tagHeader = elementHeaders.stream()
                .filter(h -> IwanModelConstants.TAG.equals(h.propertyName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(IwanModelConstants.TAG + " not found for element " + elementName + ""));

        String tag = line[tagHeader.index];

        if (tag.isEmpty()) {
            throw new NoSuchElementException("Element " + elementName + " not found in database for update");
        }

        Node node = graphDb.findNode(IwanModelConstants.LABEL_NETWORK_ELEMENT, IwanModelConstants.TAG, tag);
        if (node != null) {
            persistElementProperties(line, elementHeaders, node);
            return Optional.of(UniqueEntity.existing(node));
        } else {
            throw new NoSuchElementException("Element with tag " + tag + " not found in database for update");
        }
    }

    private static void persistElementProperties(String[] line, ImmutableCollection<HeaderElement> elementHeaders, Node elementNode) {
        elementHeaders.stream()
                .filter(h -> !IwanModelConstants.TAG.equals(h.propertyName))
                .forEach(h -> h.visit(new Visitor<Void>() {
                    @Override
                    public Void visitSimple(SimpleElementHeader header) {
                        persistElementProperty(header, line, elementNode);
                        return null;
                    }

                    @Override
                    public Void visitMulti(MultiElementHeader header) {
                        return null;
                    }
                }));
    }

    private static <T extends PropertyContainer> T persistElementProperty(HeaderElement header, String[] line, T elementNode) {
        String field = line[header.index];
        Object value = PropertyConverter.convert(field, header.type, header.isArray);
        if (value != null) {
            elementNode.setProperty(header.propertyName, value);
        } else {
            elementNode.removeProperty(header.propertyName);
        }
        return elementNode;
    }

    private ImmutableMultimap<String, Node> getPlanetsForClient(String clientAttribute) throws InvalidScopeException {
        ImmutableMultimap<String, Node> planets = planetsByClient.get(clientAttribute);
        if (planets == null) {
            try (Context ignore = metrics.timer("IWanTopologyLoader-getPlanetsForClient").time()) {
                Node attClientNode = attributeNodes.get(clientAttribute);
                if (attClientNode != null) {
                    Builder<String, Node> bldr = ImmutableMultimap.builder();
                    attClientNode.getRelationships(Direction.INCOMING, IwanModelConstants.LINK_ATTRIBUTE)
                            .forEach(pr -> collectPlanetConsumer(pr, bldr));
                    planets = bldr.build();
                    planetsByClient.put(clientAttribute, planets);
                } else {
                    throw new InvalidScopeException("The scope node " + clientAttribute + " does not exist in database.");
                }
            }
        }

        return planets;
    }

    private ImmutableMultimap<String, Node> getGlobalPlanets() {
        ImmutableMultimap<String, Node> planets = planetsByClient.get(Character.toString(IwanModelConstants.KEYTYPE_SEPARATOR));
        if (planets == null) {
            try (Context ignore = metrics.timer("IWanTopologyLoader-getGlobalPlanets").time()) {
                Node scopeGlobal = attributeNodes.get(IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE);
                Builder<String, Node> bldr = ImmutableMultimap.builder();
                scopeGlobal.getRelationships(Direction.INCOMING, IwanModelConstants.LINK_PARENT).forEach(r -> {
                    Node keyAttributeNode = r.getStartNode();
                    if (!IwanModelConstants.KEY_TYPES.contains(keyAttributeNode.getProperty(IwanModelConstants._TYPE).toString())) {
                        return;
                    }

                    keyAttributeNode.getRelationships(Direction.INCOMING, IwanModelConstants.LINK_ATTRIBUTE)
                            .forEach(pr -> collectPlanetConsumer(pr, bldr));
                });

                planets = bldr.build();
                planetsByClient.put(Character.toString(IwanModelConstants.KEYTYPE_SEPARATOR), planets);
            }
        }

        return planets;
    }

    private void collectPlanetConsumer(Relationship pr, Builder<String, Node> bldr) {
        Node planet = pr.getStartNode();
        if (!planet.hasLabel(IwanModelConstants.LABEL_PLANET)) {
            return;
        }

        planet.getRelationships(Direction.OUTGOING, IwanModelConstants.LINK_ATTRIBUTE)
                .forEach(l -> getRelationshipConsumer(l, bldr));
    }

    private Builder<String, Node> getRelationshipConsumer(Relationship l, Builder<String, Node> bldr) {
        Node planet = l.getStartNode();
        Node attribute = l.getEndNode();
        if (attribute.hasLabel(IwanModelConstants.LABEL_ATTRIBUTE)) {
            String type = attribute.getProperty(IwanModelConstants._TYPE).toString();
            if (IwanModelConstants.KEY_TYPES.contains(type)) {
                bldr.put(type + IwanModelConstants.KEYTYPE_SEPARATOR + attribute.getProperty(IwanModelConstants.NAME), planet);
            }
        }
        return bldr;
    }

}
