package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.OverridableElementFactory;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.helper.RelationshipUtils;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.model.exception.ImportException;
import com.livingobjects.neo4j.model.exception.InvalidSchemaException;
import com.livingobjects.neo4j.model.exception.InvalidScopeException;
import com.livingobjects.neo4j.model.exception.MissingElementException;
import com.livingobjects.neo4j.model.header.HeaderElement;
import com.livingobjects.neo4j.model.header.HeaderElement.Visitor;
import com.livingobjects.neo4j.model.header.MultiElementHeader;
import com.livingobjects.neo4j.model.header.SimpleElementHeader;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jLoadResult;
import com.livingobjects.neo4j.model.result.TypedScope;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.helper.RelationshipUtils.replaceRelationships;
import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.*;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.APPLIED_TO;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class CsvTopologyLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvTopologyLoader.class);
    private static final int MAX_TRANSACTION_COUNT = 500;

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final TemplatedPlanetFactory planetFactory;
    private final UniqueElementFactory networkElementFactory;
    private final UniqueElementFactory scopeElementFactory;
    private final OverridableElementFactory overridableElementFactory;
    private final ElementScopeSlider elementScopeSlider;
    private final TransactionManager txManager;
    private final MetaSchema metaSchema;
    private final TopologyLoaderUtils topologyLoaderUtils;

    public CsvTopologyLoader(GraphDatabaseService graphDb, MetricRegistry metrics) {
        this.metrics = metrics;
        this.graphDb = graphDb;
        this.txManager = new TransactionManager(graphDb);

        try (Transaction ignore = graphDb.beginTx()) {
            this.networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);
            this.scopeElementFactory = new UniqueElementFactory(graphDb, Labels.SCOPE, Optional.empty());
            this.overridableElementFactory = OverridableElementFactory.networkElementFactory(graphDb);

            this.planetFactory = new TemplatedPlanetFactory(graphDb);
            this.elementScopeSlider = new ElementScopeSlider(planetFactory);
            this.metaSchema = new MetaSchema(graphDb);

            topologyLoaderUtils = new TopologyLoaderUtils(metaSchema, scopeElementFactory);
        }
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
            try {
                LineMappingStrategy lineStrategy = strategy.reduceStrategyForLine(ImmutableSet.copyOf(metaSchema.scopeByKeyTypes.values()), nextLine);
                ImmutableSet<String> scopeKeyTypes = lineStrategy.guessKeyTypesForLine(metaSchema.scopeTypes, nextLine);
                ImmutableMultimap<TypedScope, String> importedElementByScopeInLine = importLine(nextLine, scopeKeyTypes, lineStrategy);
                LOGGER.debug("Line {} imported.", lineIndex);
                for (Entry<TypedScope, Collection<String>> importedElements : importedElementByScopeInLine.asMap().entrySet()) {
                    Set<String> set = importedElementByScope.computeIfAbsent(importedElements.getKey(), k -> Sets.newHashSet());
                    set.addAll(importedElements.getValue());
                }
                imported++;
                currentTransaction.add(nextLine);
                if (currentTransaction.size() >= MAX_TRANSACTION_COUNT) {
                    tx = txManager.renewTransaction(tx);
                    currentTransaction.clear();
                }
            } catch (ImportException e) {
                tx = renewTransaction(strategy, currentTransaction, tx);
                errors.put(lineIndex, e.getMessage());
                LOGGER.error("Line {} not imported : {}", lineIndex, e.getLocalizedMessage());
                LOGGER.error(Arrays.toString(nextLine));
            } catch (Exception e) {
                tx = renewTransaction(strategy, currentTransaction, tx);
                errors.put(lineIndex, e.getMessage());
                LOGGER.error("Line {} not imported : {}", lineIndex, e.getLocalizedMessage());
                LOGGER.error(Arrays.toString(nextLine));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("STACKTRACE", e);
                }
            }
            lineIndex++;
        }
        tx.success();
        tx.close();

        return new Neo4jLoadResult(imported, errors, importedElementByScope);
    }

    private Transaction renewTransaction(IwanMappingStrategy strategy, List<String[]> currentTransaction, Transaction tx) {
        tx = txManager.properlyRenewTransaction(tx, currentTransaction, ct -> {
            LineMappingStrategy lineStrategy = strategy.reduceStrategyForLine(ImmutableSet.copyOf(metaSchema.scopeByKeyTypes.values()), ct);
            ImmutableSet<String> scopeKeyTypes = lineStrategy.guessKeyTypesForLine(metaSchema.scopeTypes, ct);
            importLine(ct, scopeKeyTypes, lineStrategy);
        });
        return tx;
    }

    private ImmutableMultimap<TypedScope, String> importLine(String[] line, ImmutableSet<String> scopeKeytypes, LineMappingStrategy strategy) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {
            ImmutableMap<String, Set<String>> lineage = strategy.guessElementCreationStrategy(scopeKeytypes, metaSchema.childrenRelations);

            // Update the elements in the CSV line that cannot be created in any case (because required parent are missing in the CSV line)
            Map<String, Optional<UniqueEntity<Node>>> elementsWithoutParents = strategy.getAllElementsType().stream()
                    .filter(key -> !lineage.keySet().contains(key))
                    .filter(key -> !SCOPE_CLASS.equals(key))
                    .map(key -> Maps.immutableEntry(key, updateElement(strategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            // Create elements
            Map<String, Optional<UniqueEntity<Node>>> elementsWithParents = lineage.keySet().stream()
                    .map(key -> Maps.immutableEntry(key, createElement(strategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            Map<String, Optional<UniqueEntity<Node>>> nodesBuilder = Maps.newHashMap();
            nodesBuilder.putAll(elementsWithoutParents);
            nodesBuilder.putAll(elementsWithParents);
            nodesBuilder.put(SCOPE_GLOBAL_ATTRIBUTE, Optional.of(UniqueEntity.existing(metaSchema.getTheGlobalScopeNode())));

            ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes = ImmutableMap.copyOf(nodesBuilder);

            createCrossAttributeLinks(line, strategy, nodes);

            createConnectLink(strategy, nodes);

            return createOrUpdatePlanetLink(strategy, nodes);
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
            ImmutableSet<String> crossAttributesLinks = metaSchema.getCrossAttributesRelations(meHeader.elementName);
            if (crossAttributesLinks == null || !crossAttributesLinks.contains(meHeader.targetElementName)) {
                throw new InvalidSchemaException("Schema does not allow to create cross attribute between '" +
                        meHeader.elementName + "' and '" + meHeader.targetElementName + "'. Import aborted.");
            }
        }
    }

    private Map<String, Relationship> createCrossAttributeRelationships(Map<String, Optional<UniqueEntity<Node>>> nodes) {
        Map<String, Relationship> multiElementLinks = Maps.newHashMap();
        for (Entry<String, ImmutableSet<String>> rel : metaSchema.crossAttributesRelations.entrySet()) {
            String keyType = rel.getKey();
            Optional<UniqueEntity<Node>> startNode = nodes.getOrDefault(keyType, Optional.empty());
            startNode.ifPresent(fromNode -> {
                for (String endKeyType : rel.getValue()) {
                    if (!keyType.equals(endKeyType)) {
                        Optional<UniqueEntity<Node>> optEndNode = nodes.getOrDefault(endKeyType, Optional.empty());
                        optEndNode.ifPresent(toNode -> {
                            Relationship link = createOutgoingUniqueLink(fromNode.entity, toNode.entity, RelationshipTypes.CROSS_ATTRIBUTE);
                            String key = keyType + ELEMENT_SEPARATOR + endKeyType;
                            multiElementLinks.put(key, link);
                        });
                    }
                }
            });
        }
        return multiElementLinks;
    }

    private void createConnectLink(LineMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        nodes.forEach((keyType, oNode) ->
                oNode.ifPresent(node -> linkToParents(strategy, keyType, node, nodes)));
    }

    private ImmutableMultimap<TypedScope, String> createOrUpdatePlanetLink(LineMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        ImmutableMultimap.Builder<TypedScope, String> importedElementByScopeBuilder = ImmutableMultimap.builder();
        for (Entry<String, Optional<UniqueEntity<Node>>> node : nodes.entrySet()) {
            if (!node.getValue().isPresent()) continue;
            UniqueEntity<Node> element = node.getValue().get();
            String keyAttribute = node.getKey();

            if (GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(keyAttribute)) continue;

            importedElementByScopeBuilder.put(
                    reviewPlanetElement(strategy, element, nodes),
                    element.entity.getProperty(GraphModelConstants.TAG).toString()
            );
        }
        return importedElementByScopeBuilder.build();
    }

    private TypedScope reviewPlanetElement(LineMappingStrategy strategy,
                                           UniqueEntity<Node> element,
                                           Map<String, Optional<UniqueEntity<Node>>> nodes) {
        String keyType = element.entity.getProperty(_TYPE).toString();
        boolean isOverridable = metaSchema.isOverridable(keyType);
        boolean isGlobal = Optional.ofNullable(metaSchema.scopeByKeyTypes.get(keyType))
                .map(SCOPE_GLOBAL_ATTRIBUTE::equals)
                .orElse(false);
        Scope scopeFromImport = isGlobal ? GLOBAL_SCOPE : strategy.scope;

        Scope scopeFromDatabase = topologyLoaderUtils.getScopeFromElementPlanet(element.entity)
                .orElseGet(() -> !isOverridable ? getScopeFromParent(strategy, keyType, nodes).orElse(null) : null);

        if (scopeFromImport != null) {
            if (scopeFromDatabase != null && !scopeFromImport.tag.equals(scopeFromDatabase.tag)) {
                // If the imported element scope is different than the existing one
                // slide the element to the new scope
                elementScopeSlider.slide(element.entity, scopeFromImport);
            }
            UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scopeFromImport, element.entity);
            replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
            return new TypedScope(scopeFromImport.tag, keyType);
        } else {
            if (scopeFromDatabase == null) {
                Object tag = element.entity.getProperty(TAG);
                throw new IllegalStateException(String.format("Inconsistent element '%s' in db : it is not linked to a planet which is required. Fix this.", tag));
            } else {
                // review the planet (in case the element has been created)
                UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scopeFromDatabase, element.entity);
                replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
                return new TypedScope(scopeFromDatabase.tag, keyType);
            }
        }
    }

    private Optional<Scope> getScopeFromParent(LineMappingStrategy strategy, String keyAttribute, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        return metaSchema.getRequiredParent(keyAttribute)
                .flatMap(requiredParent ->
                        nodes.getOrDefault(requiredParent, Optional.empty())
                                .flatMap(nodeUniqueEntity -> topologyLoaderUtils.getScopeFromElementPlanet(nodeUniqueEntity.entity)));
    }

    private int linkToParents(LineMappingStrategy strategy, String keyType, UniqueEntity<Node> keyTypeNode, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-linkToParents").time()) {
            ImmutableList<Relationship> relationships = metaSchema.parentRelations.get(keyType);
            if (relationships == null || relationships.isEmpty()) {
                return -1;
            }
            boolean isOverridable = metaSchema.isOverridable(keyType);

            int relCount = 0;
            for (Relationship relationship : relationships) {
                Node endNode = relationship.getEndNode();
                String toKeytype = endNode.getProperty(GraphModelConstants._TYPE).toString() + GraphModelConstants.KEYTYPE_SEPARATOR +
                        endNode.getProperty(NAME).toString();

                if (metaSchema.isScope(toKeytype) && !toKeytype.equals(strategy.scope.attribute)) {
                    continue;
                }

                // if (isOverridable) {
                //     toKeytype = strategy.scope.attribute;
                // }

                Optional<UniqueEntity<Node>> parent = nodes.get(toKeytype);
                if (parent == null || !parent.isPresent()) {
                    String cardinality = relationship.getProperty(GraphModelConstants.CARDINALITY, GraphModelConstants.CARDINALITY_UNIQUE_PARENT).toString();
                    if (keyTypeNode.wasCreated && GraphModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
                        Object tagProperty = keyTypeNode.entity.getProperty(TAG);
                        throw new MissingElementException(String.format("Unable to update '%s' because the element does not exist : '%s'. Line is ignored.", keyType, tagProperty));
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

    private Relationship createOutgoingUniqueLink(Node node, Node parent, org.neo4j.graphdb.RelationshipType linkType) {
        for (Relationship next : node.getRelationships(Direction.OUTGOING, linkType)) {
            if (next.getEndNode().equals(parent)) {
                return next;
            }
        }
        return node.createRelationshipTo(parent, linkType);
    }

    private Optional<UniqueEntity<Node>> createElement(LineMappingStrategy strategy, String[] line, String elementKeyType) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createElement").time()) {
            if (GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(elementKeyType)) {
                return Optional.of(UniqueEntity.existing(graphDb.findNode(Labels.SCOPE, "tag", GraphModelConstants.SCOPE_GLOBAL_TAG)));
            }

            boolean isOverridable = metaSchema.isOverridable(elementKeyType);

            Set<String> todelete = metaSchema.getMonoParentRelations(elementKeyType)
                    .filter(strategy::hasKeyType)
                    .collect(Collectors.toSet());

            int tagIndex = strategy.getColumnIndex(elementKeyType, GraphModelConstants.TAG);
            if (tagIndex < 0) return Optional.empty();
            String tag = line[tagIndex];
            if (tag.isEmpty()) return Optional.empty();

            boolean isScope = metaSchema.isScope(elementKeyType);
            UniqueEntity<Node> uniqueEntity = (isOverridable) ?
                    overridableElementFactory.getOrOverride(strategy.scope, GraphModelConstants.TAG, tag) :
                    networkElementFactory.getOrCreateWithOutcome(GraphModelConstants.TAG, tag);
            Node elementNode = uniqueEntity.entity;

            Iterable<String> schemasToApply = getSchemasToApply(strategy, line, elementKeyType);
            if (uniqueEntity.wasCreated) {
                if (isScope) {
                    int nameIndex = strategy.getColumnIndex(elementKeyType, NAME);
                    if (nameIndex < 0) return Optional.empty();
                    String name = line[nameIndex];
                    if (name.isEmpty()) {
                        uniqueEntity.entity.delete();
                        return Optional.empty();
                    }

                    elementNode.addLabel(Labels.SCOPE);
                    if (Iterables.isEmpty(schemasToApply)) {
                        throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + GraphModelConstants.SCHEMA));
                    }
                    applySchemas(elementKeyType, elementNode, schemasToApply);
                }
                elementNode.setProperty(GraphModelConstants._TYPE, elementKeyType);

            } else {
                if (isScope) {
                    if (!Iterables.isEmpty(schemasToApply)) {
                        applySchema(strategy, line, elementKeyType, tag, elementNode);
                    }
                }
                elementNode.setProperty(GraphModelConstants.UPDATED_AT, Instant.now().toEpochMilli());

                elementNode.getRelationships(Direction.OUTGOING, RelationshipTypes.CONNECT).forEach(r -> {
                    String type = r.getEndNode().getProperty(GraphModelConstants._TYPE, GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE).toString();
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
            Iterable<String> schemas = getSchemasToApply(strategy, line, elementKeyType);
            applySchemas(elementKeyType, elementNode, schemas);
        } catch (NoSuchElementException e) {
            throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + GraphModelConstants.SCHEMA));
        }
    }

    private Iterable<String> getSchemasToApply(IwanMappingStrategy strategy, String[] line, String elementKeyType) {
        return strategy.tryColumnIndex(elementKeyType, GraphModelConstants.SCHEMA)
                .map(schemaIndex -> Splitter.on(',').omitEmptyStrings().trimResults().split(line[schemaIndex]))
                .orElse(ImmutableSet.of());
    }

    private void applySchemas(String elementKeyType, Node elementNode, Iterable<String> schemas) {
        Set<Node> schemaNodes = Sets.newHashSet();
        schemas.forEach(schema -> {
            Node schemaNode = graphDb.findNode(Labels.SCHEMA, GraphModelConstants.ID, schema);
            if (schemaNode != null) {
                schemaNodes.add(schemaNode);
            } else {
                throw new InvalidScopeException(String.format("Unable to apply schema '%s' for node '%s'. Schema not found.", schema, elementKeyType));
            }
        });
        RelationshipUtils.updateRelationships(INCOMING, elementNode, APPLIED_TO, schemaNodes);
    }

    private Optional<UniqueEntity<Node>> updateElement(LineMappingStrategy strategy, String[] line, String elementName) throws NoSuchElementException {
        boolean isOverridable = metaSchema.isOverridable(elementName);
        if (isOverridable && !SCOPE_GLOBAL_TAG.equals(strategy.scope.tag)) {
            return createElement(strategy, line, elementName);
        }

        ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementName);
        HeaderElement tagHeader = elementHeaders.stream()
                .filter(h -> GraphModelConstants.TAG.equals(h.propertyName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(GraphModelConstants.TAG + " not found for element " + elementName + ""));

        String tag = line[tagHeader.index];

        if (tag.isEmpty()) {
            throw new NoSuchElementException("Element " + elementName + " not found in database for update");
        }

        Node node = graphDb.findNode(Labels.NETWORK_ELEMENT, GraphModelConstants.TAG, tag);
        if (node != null) {
            persistElementProperties(line, elementHeaders, node);
            return Optional.of(UniqueEntity.existing(node));
        } else {
            throw new NoSuchElementException("Element with tag " + tag + " not found in database for update");
        }
    }

    private static void persistElementProperties(String[] line, ImmutableCollection<HeaderElement> elementHeaders, Node elementNode) {
        elementHeaders.stream()
                .filter(h -> !GraphModelConstants.TAG.equals(h.propertyName))
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
