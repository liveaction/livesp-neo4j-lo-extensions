package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.OverridableElementFactory;
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
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jLoadResult;
import com.livingobjects.neo4j.model.result.TypedScope;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

public final class IWanTopologyLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(IWanTopologyLoader.class);
    private static final int MAX_TRANSACTION_COUNT = 500;

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final UniqueElementFactory planetFactory;
    private final UniqueElementFactory networkElementFactory;
    private final OverridableElementFactory overridableElementFactory;

    private final Node theGlobalNode;
    private final ImmutableMap<String, Node> attributeNodes;
    private final ImmutableSet<String> overridableType;
    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    private final ImmutableMap<String, ImmutableSet<String>> crossAttributesRelations;
    private final ImmutableSet<String> scopeTypes;
    private final ImmutableMap<String, String> scopeByKeyTypes;

    private final Map<String, String> planetNameTemplateCache = Maps.newConcurrentMap();

    public IWanTopologyLoader(GraphDatabaseService graphDb, MetricRegistry metrics) {
        this.metrics = metrics;
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {

            planetFactory = UniqueElementFactory.planetFactory(graphDb);
            networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);
            overridableElementFactory = OverridableElementFactory.networkElementFactory(graphDb);

            this.theGlobalNode = graphDb.findNode(Labels.SCOPE, TAG, GLOBAL_SCOPE.tag);
            if (this.theGlobalNode == null) {
                throw new IllegalStateException("Global Scope node not found in database !");
            }

            ImmutableMap.Builder<String, Node> importableKeyTypesBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, Node> attributesBldr = ImmutableMap.builder();
            ImmutableSet.Builder<String> overrideBldr = ImmutableSet.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableSet<String>> crossAttributesRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<Node, String> scopesBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, String> scopeByKeyTypesBldr = ImmutableMap.builder();
            graphDb.findNodes(Labels.ATTRIBUTE).forEachRemaining(n -> {
                String keytype = n.getProperty(IwanModelConstants._TYPE).toString();
                String key = keytype + IwanModelConstants.KEYTYPE_SEPARATOR + n.getProperty(IwanModelConstants.NAME).toString();
                boolean isOverride = (boolean) n.getProperty(_OVERRIDABLE, false);
                attributesBldr.put(key, n);
                if (isOverride) overrideBldr.add(key);

                if (IwanModelConstants.KEY_TYPES.contains(keytype)) {
                    if (IwanModelConstants.IMPORTABLE_KEY_TYPES.contains(keytype)) {
                        importableKeyTypesBldr.put(key, n);
                    }
                    ImmutableList.Builder<Relationship> crels = ImmutableList.builder();
                    ImmutableList.Builder<Relationship> prels = ImmutableList.builder();
                    ImmutableSet.Builder<String> crossRels = ImmutableSet.builder();
                    n.getRelationships(Direction.INCOMING, RelationshipTypes.PARENT).forEach(crels::add);
                    n.getRelationships(Direction.OUTGOING, RelationshipTypes.PARENT).forEach(prels::add);
                    n.getRelationships(Direction.OUTGOING, RelationshipTypes.CROSS_ATTRIBUTE).forEach(r -> {
                        Node endNode = r.getEndNode();
                        if (endNode.hasLabel(Labels.ATTRIBUTE)) {
                            Object typeProperty = endNode.getProperty(IwanModelConstants._TYPE);
                            Object nameProperty = endNode.getProperty(IwanModelConstants.NAME);
                            if (typeProperty != null && nameProperty != null) {
                                String endKeytype = typeProperty.toString() + IwanModelConstants.KEYTYPE_SEPARATOR + nameProperty.toString();
                                crossRels.add(endKeytype);
                            }
                        }
                    });
                    if (!IwanModelConstants.LABEL_TYPE.equals(keytype) && prels.build().isEmpty()) {
                        scopesBldr.put(n, key);
                    }
                    childrenRelationsBldr.put(key, crels.build());
                    parentRelationsBldr.put(key, prels.build());
                    crossAttributesRelationsBldr.put(key, crossRels.build());
                }
            });

            this.attributeNodes = attributesBldr.build();
            this.overridableType = overrideBldr.build();
            this.childrenRelations = childrenRelationsBldr.build();
            this.parentRelations = parentRelationsBldr.build();
            this.crossAttributesRelations = crossAttributesRelationsBldr.build();

            ImmutableMap<Node, String> scopes = scopesBldr.build();
            for (Entry<String, Node> attributeNodeEntry : importableKeyTypesBldr.build().entrySet()) {
                String keyType = attributeNodeEntry.getKey();
                Node attributeNode = attributeNodeEntry.getValue();
                getScopeContext(scopes, attributeNode)
                        .ifPresent(scope -> scopeByKeyTypesBldr.put(keyType, scope));
            }
            this.scopeByKeyTypes = scopeByKeyTypesBldr.build();

            this.scopeTypes = ImmutableSet.<String>builder()
                    .addAll(scopes.values())
                    .build();
        }
    }

    private Optional<String> getScopeContext(ImmutableMap<Node, String> scopes, Node attributeNode) {
        return getParent(attributeNode)
                .map(node -> getScopeContext(scopes, node))
                .orElseGet(() -> Optional.ofNullable(scopes.get(attributeNode)));
    }

    private Optional<Node> getParent(Node attributeNode) {
        for (Relationship relationship : attributeNode.getRelationships(Direction.OUTGOING, RelationshipTypes.PARENT)) {
            if (IwanModelConstants.CARDINALITY_UNIQUE_PARENT.equals(relationship.getProperty(IwanModelConstants.CARDINALITY, IwanModelConstants.CARDINALITY_UNIQUE_PARENT))) {
                return Optional.of(relationship.getEndNode());
            }
        }
        return Optional.empty();
    }

    public Neo4jLoadResult loadFromStream(InputStream is) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(is));
        IwanMappingStrategy strategy = IwanMappingStrategy.captureHeader(reader);

        checkCrossAttributeDefinitionExists(strategy);

        String[] nextLine;

        int lineIndex = 0;
        int imported = 0;
        Map<TypedScope, Set<String>> importedElementByScope = Maps.newHashMap();
        List<String[]> currentTransaction = Lists.newArrayListWithCapacity(MAX_TRANSACTION_COUNT);
        Map<Integer, String> errors = Maps.newHashMap();

        Transaction tx = graphDb.beginTx();
        while ((nextLine = reader.readNext()) != null) {
            LineMappingStrategy lineStrategy = strategy.reduceStrategyForLine(ImmutableSet.copyOf(scopeByKeyTypes.values()), nextLine);
            ImmutableSet<String> scopeKeyTypes = lineStrategy.guessKeyTypesForLine(scopeTypes, nextLine);
            try {
                ImmutableMultimap<TypedScope, String> importedElementByScopeInLine = importLine(nextLine, scopeKeyTypes, lineStrategy);
                for (Entry<TypedScope, Collection<String>> importedElements : importedElementByScopeInLine.asMap().entrySet()) {
                    Set<String> set = importedElementByScope.computeIfAbsent(importedElements.getKey(), k -> Sets.newHashSet());
                    set.addAll(importedElements.getValue());
                }
                imported++;
                currentTransaction.add(nextLine);
                if (currentTransaction.size() >= MAX_TRANSACTION_COUNT) {
                    tx = renewTransaction(tx);
                    currentTransaction.clear();
                }
            } catch (ImportException | NoSuchElementException e) {
                tx = properlyRenewTransaction(strategy, currentTransaction, tx);
                errors.put(lineIndex, e.getMessage());
                LOGGER.warn(e.getLocalizedMessage());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("STACKTRACE", e);
                    LOGGER.debug(Arrays.toString(nextLine));
                }

            } catch (IllegalArgumentException e) {
                tx = properlyRenewTransaction(strategy, currentTransaction, tx);
                errors.put(lineIndex, e.getMessage());
                LOGGER.error(e.getLocalizedMessage());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("STACKTRACE", e);
                    LOGGER.debug(Arrays.toString(nextLine));
                }
            }
            lineIndex++;
        }
        tx.success();
        tx.close();

        return new Neo4jLoadResult(imported, errors, importedElementByScope);
    }

    private Transaction properlyRenewTransaction(IwanMappingStrategy strategy, List<String[]> currentTransaction, Transaction tx) {
        tx = renewTransaction(tx, true);
        tx = reloadValidTransactionLines(tx, currentTransaction, strategy);
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
            Transaction tx, List<String[]> lines, IwanMappingStrategy strategy) {
        if (!lines.isEmpty()) {
            lines.forEach(ct -> {
                LineMappingStrategy lineStrategy = strategy.reduceStrategyForLine(ImmutableSet.copyOf(scopeByKeyTypes.values()), ct);
                ImmutableSet<String> scopeKeyTypes = lineStrategy.guessKeyTypesForLine(scopeTypes, ct);
                importLine(ct, scopeKeyTypes, lineStrategy);
            });
            return renewTransaction(tx);
        }
        return tx;
    }

    private ImmutableMultimap<TypedScope, String> importLine(String[] line, ImmutableSet<String> scopeKeytypes, LineMappingStrategy strategy) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {
            ImmutableMap<String, Set<String>> lineage = strategy.guessElementCreationStrategy(scopeKeytypes, childrenRelations);

            // Try to update elements which is not possible to create (no parent founds)
            // updated elements must exist or NoSuchElement was throw
            ImmutableMap.Builder<String, Optional<UniqueEntity<Node>>> allNodesBldr = ImmutableMap.builder();
            strategy.getAllElementsType().stream()
                    .filter(key -> !lineage.keySet().contains(key))
                    .filter(key -> !SCOPE_CLASS.equals(key))
                    .forEach(key -> allNodesBldr.put(key, updateElement(strategy, line, key)));

            // Create elements
            Map<String, Optional<UniqueEntity<Node>>> nodes = lineage.keySet().stream()
                    .map(key -> Maps.immutableEntry(key, createElement(strategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            nodes.put(SCOPE_GLOBAL_ATTRIBUTE, Optional.of(UniqueEntity.existing(theGlobalNode)));

            ImmutableMap<String, Optional<UniqueEntity<Node>>> allNodes =
                    allNodesBldr
                            .putAll(nodes)
                            .build();

            createCrossAttributeLinks(line, strategy, allNodes);

            createConnectLink(strategy, nodes);

            return createPlanetLink(strategy, nodes);
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
                throw new InvalidSchemaException("Schema does not allow to create cross attribute between '" +
                        meHeader.elementName + "' and '" + meHeader.targetElementName + "'. Import aborted.");
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
                        Relationship link = createOutgoingUniqueLink(fromNode.entity, toNode.entity, RelationshipTypes.CROSS_ATTRIBUTE);
                        String key = keyType + ELEMENT_SEPARATOR + endKeyType;
                        multiElementLinks.put(key, link);
                    });
                }
            });
        }
        return multiElementLinks;
    }

    private void createConnectLink(LineMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        nodes.forEach((keyType, oNode) ->
                oNode.ifPresent(node -> linkToParents(strategy, keyType, node, nodes)));
    }

    private Node getNextStartNode(Iterator<Relationship> relationshipIterator, Label label) {
        while (relationshipIterator.hasNext()) {
            Relationship next = relationshipIterator.next();
            Node endNode = next.getStartNode();
            for (Label currentLabel : endNode.getLabels()) {
                if (currentLabel.equals(label)) {
                    return endNode;
                }
            }
        }
        return null;
    }

    private ImmutableMultimap<TypedScope, String> createPlanetLink(LineMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        if (strategy.scope == null)
            throw new IllegalArgumentException("Unable to import element. No scope found for line");

        ImmutableMultimap.Builder<TypedScope, String> importedElementByScopeBuilder = ImmutableMultimap.builder();
        for (Entry<String, Optional<UniqueEntity<Node>>> node : nodes.entrySet()) {
            node.getValue().ifPresent(element -> {
                        String keyType = node.getKey();
                        if (IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(keyType)) return;

                        String planetTemplateName = planetNameTemplateCache.computeIfAbsent(keyType, this::loadPlanetTemplateName);
                        String planetName = planetTemplateName.replace("{:scopeId}", strategy.scope.id);
                        UniqueEntity<Node> planet = planetFactory.getOrCreateWithOutcome(IwanModelConstants.NAME, planetName);
                        AtomicBoolean present = new AtomicBoolean(false);
                        element.entity.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING).forEach(r -> {
                            if (r.getEndNode().getId() == planet.entity.getId()) {
                                present.set(true);
                            } else {
                                r.delete();
                            }
                        });
                        if (!present.get()) {
                            element.entity.createRelationshipTo(planet.entity, RelationshipTypes.ATTRIBUTE);
                        }
                        importedElementByScopeBuilder.put(new TypedScope(strategy.scope.tag, keyType), element.entity.getProperty(IwanModelConstants.TAG).toString());
                    }
            );
        }
        return importedElementByScopeBuilder.build();
    }

    private String loadPlanetTemplateName(String keyType) {
        Node attributeNode = attributeNodes.get(keyType);
        Iterator<Relationship> iterator = attributeNode.getRelationships(Direction.INCOMING, RelationshipTypes.ATTRIBUTE).iterator();
        Node planetTemplate = getNextStartNode(iterator, Labels.PLANET_TEMPLATE);

        if (planetTemplate != null) {
            if (getNextStartNode(iterator, Labels.PLANET_TEMPLATE) != null) {
                throw new IllegalStateException(String.format("Unable to instantiate planet for '%s'. Found more than one PlanetTemplate.", keyType));
            }
            Object template = planetTemplate.getProperty("name", null);
            if (template != null) {
                return template.toString();
            } else {
                throw new IllegalStateException(String.format("Unable to instantiate planet for '%s'. No PlanetTemplate.name property found.", keyType));
            }
        } else {
            throw new IllegalStateException(String.format("Unable to instantiate planet for '%s'. No PlanetTemplate found.", keyType));
        }
    }

    private int linkToParents(LineMappingStrategy strategy, String keyType, UniqueEntity<Node> keyTypeNode, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-linkToParents").time()) {
            ImmutableList<Relationship> relationships = parentRelations.get(keyType);
            if (relationships == null || relationships.isEmpty()) {
                return -1;
            }
            boolean isOverride = overridableType.contains(keyType);

            int relCount = 0;
            for (Relationship relationship : relationships) {
                Node endNode = relationship.getEndNode();
                String toKeytype = endNode.getProperty(IwanModelConstants._TYPE).toString() + IwanModelConstants.KEYTYPE_SEPARATOR +
                        endNode.getProperty(IwanModelConstants.NAME).toString();
                if (isOverride && scopeTypes.contains(toKeytype))
                    toKeytype = strategy.scope.attribute;

                Optional<UniqueEntity<Node>> parent = nodes.get(toKeytype);
                if (parent == null || !parent.isPresent()) {
                    String cardinality = relationship.getProperty(IwanModelConstants.CARDINALITY, IwanModelConstants.CARDINALITY_UNIQUE_PARENT).toString();
                    if (keyTypeNode.wasCreated && IwanModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
                        throw new IllegalArgumentException(String.format("Unable to import '%s' missing parent '%s'.", keyType, toKeytype));
                    } else {
                        continue;
                    }
                }
                ++relCount;
                createOutgoingUniqueLink(keyTypeNode.entity, parent.get().entity, RelationshipTypes.CONNECT);
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

    private Optional<UniqueEntity<Node>> createElement(LineMappingStrategy strategy, String[] line, String elementKeyType) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createElement").time()) {
            if (IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(elementKeyType)) {
                return Optional.of(UniqueEntity.existing(graphDb.findNode(Labels.SCOPE, "tag", IwanModelConstants.SCOPE_GLOBAL_TAG)));
            }

            boolean isOverridable = overridableType.contains(elementKeyType);

            Set<String> todelete = parentRelations.get(elementKeyType).stream()
                    .filter(r -> !IwanModelConstants.CARDINALITY_MULTIPLE.equals(r.getProperty(IwanModelConstants.CARDINALITY, "")))
                    .map(r -> r.getEndNode().getProperty(IwanModelConstants._TYPE).toString() + IwanModelConstants.KEYTYPE_SEPARATOR + r.getEndNode().getProperty(IwanModelConstants.NAME).toString())
                    .filter(strategy::hasKeyType)
                    .collect(Collectors.toSet());

            int tagIndex = strategy.getColumnIndex(elementKeyType, IwanModelConstants.TAG);
            if (tagIndex < 0) return Optional.empty();

            String tag = line[tagIndex];
            if (tag.isEmpty()) return Optional.empty();

            UniqueEntity<Node> uniqueEntity = (isOverridable) ?
                    overridableElementFactory.getOrOverride(strategy.scope, IwanModelConstants.TAG, tag) :
                    networkElementFactory.getOrCreateWithOutcome(IwanModelConstants.TAG, tag);
            Node elementNode = uniqueEntity.entity;

            if (uniqueEntity.wasCreated) {
                if (scopeTypes.contains(elementKeyType)) {
                    elementNode.addLabel(Labels.SCOPE);
                    applySchema(strategy, line, elementKeyType, tag, elementNode);
                }
                elementNode.setProperty(IwanModelConstants._TYPE, elementKeyType);

            } else {
                elementNode.setProperty(IwanModelConstants.UPDATED_AT, Instant.now().toEpochMilli());

                elementNode.getRelationships(Direction.OUTGOING, RelationshipTypes.CONNECT).forEach(r -> {
                    String type = r.getEndNode().getProperty(IwanModelConstants._TYPE, IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE).toString();
                    if (todelete.contains(type)) {
                        r.delete();
                    }
                });
            }
            ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementKeyType);
            persistElementProperties(line, elementHeaders, elementNode);

            return Optional.of(uniqueEntity);
        }
    }

    private void applySchema(IwanMappingStrategy strategy, String[] line, String elementKeyType, String tag, Node elementNode) {
        try {
            int schemaIndex = strategy.getColumnIndex(elementKeyType, IwanModelConstants.SCHEMA);
            String schema = line[schemaIndex];
            Node schemaNode = graphDb.findNode(Labels.SCHEMA, IwanModelConstants.ID, schema);
            if (schemaNode != null) {
                schemaNode.createRelationshipTo(elementNode, RelationshipTypes.APPLIED_TO);
            } else {
                throw new InvalidScopeException(String.format("Unable to apply schema '%s' for node '%s'. Schema not found.", schema, elementKeyType));
            }
        } catch (NoSuchElementException e) {
            throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + IwanModelConstants.SCHEMA));
        }
    }

    private Optional<UniqueEntity<Node>> updateElement(LineMappingStrategy strategy, String[] line, String elementName) throws NoSuchElementException {
        boolean isOverridable = overridableType.contains(elementName);
        if (isOverridable && !SCOPE_GLOBAL_TAG.equals(strategy.scope.tag)) {
            return createElement(strategy, line, elementName);
        }

        ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementName);
        HeaderElement tagHeader = elementHeaders.stream()
                .filter(h -> IwanModelConstants.TAG.equals(h.propertyName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(IwanModelConstants.TAG + " not found for element " + elementName + ""));

        String tag = line[tagHeader.index];

        if (tag.isEmpty()) {
            throw new NoSuchElementException("Element " + elementName + " not found in database for update");
        }

        Node node = graphDb.findNode(Labels.NETWORK_ELEMENT, IwanModelConstants.TAG, tag);
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

    private static <T extends PropertyContainer> void persistElementProperty(HeaderElement header, String[] line, T elementNode) {
        String field = line[header.index];
        Object value = PropertyConverter.convert(field, header.type, header.isArray);
        if (value != null) {
            elementNode.setProperty(header.propertyName, value);
        } else {
            elementNode.removeProperty(header.propertyName);
        }
    }

}
