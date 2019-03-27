package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import com.livingobjects.neo4j.helper.*;
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
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.livingobjects.neo4j.helper.RelationshipUtils.replaceRelationships;
import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.*;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.APPLIED_TO;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class CsvTopologyLoader {

    private static final int MAX_TRANSACTION_COUNT = 500;
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvTopologyLoader.class);

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final TemplatedPlanetFactory planetFactory;
    private final UniqueElementFactory networkElementFactory;
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
            UniqueElementFactory scopeElementFactory = new UniqueElementFactory(graphDb, Labels.SCOPE, Optional.empty());
            this.overridableElementFactory = OverridableElementFactory.networkElementFactory(graphDb);

            this.planetFactory = new TemplatedPlanetFactory(graphDb);
            this.elementScopeSlider = new ElementScopeSlider(planetFactory);
            this.metaSchema = new MetaSchema(graphDb);

            topologyLoaderUtils = new TopologyLoaderUtils(scopeElementFactory);
        }
    }

    public Neo4jLoadResult loadFromStream(InputStream is) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(is));
        CsvMappingStrategy strategy = CsvMappingStrategy.captureHeader(reader);

        checkKeyAttributesExist(strategy);
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
                ImmutableSet<String> scopeKeyTypes = strategy.guessKeyTypesForLine(metaSchema.getScopeTypes(), nextLine);
                ImmutableMultimap<TypedScope, String> importedElementByScopeInLine = importLine(nextLine, scopeKeyTypes, strategy);
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
                LOGGER.debug(e.getLocalizedMessage());
                LOGGER.debug(Arrays.toString(nextLine));
            } catch (Exception e) {
                tx = renewTransaction(strategy, currentTransaction, tx);
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

    private void checkKeyAttributesExist(CsvMappingStrategy strategy) throws IOException {
        for (String keyAttribute : strategy.getAllElementsType()) {
            if (!metaSchema.keyAttributeExists(keyAttribute)) {
                throw new IOException(String.format("Column '%s' is not a known keyAttribute.", keyAttribute));
            }
        }
    }

    private Transaction renewTransaction(CsvMappingStrategy strategy, List<String[]> currentTransaction, Transaction tx) {
        tx = txManager.properlyRenewTransaction(tx, currentTransaction, ct -> {
            ImmutableSet<String> scopeKeyTypes = strategy.guessKeyTypesForLine(metaSchema.getScopeTypes(), ct);
            importLine(ct, scopeKeyTypes, strategy);
        });
        return tx;
    }

    private ImmutableMultimap<TypedScope, String> importLine(String[] line, ImmutableSet<String> scopeKeytypes, CsvMappingStrategy strategy) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {
            ImmutableMap<String, Set<String>> lineage = strategy.guessElementCreationStrategy(scopeKeytypes, metaSchema);
            LineMappingStrategy lineStrategy = new LineMappingStrategy(strategy, line);

            Map<String, Action> markedToDelete = strategy.getAllElementsType().stream()
                    .map(keyType -> lineStrategy.strategy.tryColumnIndex(keyType, Action.STATUS_HEADER)
                            .map(statusIndex -> line[statusIndex])
                            .flatMap(Action::fromString)
                            .map(action -> Maps.immutableEntry(keyType, action)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            if (markedToDelete.values().stream()
                    .distinct()
                    .count() > 1) {
                throw new IllegalArgumentException("Several elements have been marked to delete with different strategy : " + markedToDelete);
            }


            Set<String> allElementToDeleteBld = ImmutableSet.of();
            if (!markedToDelete.isEmpty()) {
                Action action = markedToDelete.values().iterator().next(); // We have checked that only one distinct action is present
                switch (action) {
                    case DELETE_CASCADE_ALL:
                        allElementToDeleteBld = markedToDelete.keySet().stream()
                                .flatMap(elt -> Stream.concat(Stream.of(elt), metaSchema.getAllChildren(elt).stream()))
                                .collect(Collectors.toSet());
                        break;
                    case DELETE_CASCADE:
                        allElementToDeleteBld = markedToDelete.keySet().stream()
                                .flatMap(elt -> Stream.concat(Stream.of(elt), metaSchema.getStrongChildren(elt).stream()))
                                .collect(Collectors.toSet());
                        break;
                    case DELETE_NO_CASCADE:
                        allElementToDeleteBld = markedToDelete.keySet();
                        break;
                }
            }
            Set<String> allElementToDelete = ImmutableSet.copyOf(allElementToDeleteBld);

            // Update the elements in the CSV line that cannot be created in any case (because required parent are missing in the CSV line)
            Map<String, Optional<UniqueEntity<Node>>> elementsWithoutParents = Sets.difference(strategy.getAllElementsType(), lineage.keySet()).stream()
                    .filter(key -> !allElementToDelete.contains(key))
                    .map(key -> Maps.immutableEntry(key, updateElement(lineStrategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            // Create elements
            Map<String, Optional<UniqueEntity<Node>>> elementsWithParents = lineage.keySet().stream()
                    .filter(key -> !allElementToDelete.contains(key))
                    .map(key -> Maps.immutableEntry(key, createElement(lineStrategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            Map<String, Optional<UniqueEntity<Node>>> nodesBuilder = Maps.newHashMap();
            nodesBuilder.putAll(elementsWithoutParents);
            nodesBuilder.putAll(elementsWithParents);
            markedToDelete.keySet().forEach(keyType -> nodesBuilder.put(keyType, Optional.empty()));
            nodesBuilder.put(SCOPE_GLOBAL_ATTRIBUTE, Optional.of(UniqueEntity.existing(metaSchema.getTheGlobalScopeNode())));

            ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes = ImmutableMap.copyOf(nodesBuilder);

            createCrossAttributeLinks(line, strategy, nodes);

            createConnectLink(lineStrategy, nodes);

            checkRequiredProperties(nodes);

            if (!markedToDelete.isEmpty()) {
                deleteElements(lineStrategy, line, markedToDelete.keySet(), markedToDelete.values().iterator().next()); // We have checked that only one distinct action is present
            }

            return createOrUpdatePlanetLink(lineStrategy, nodes);
        }
    }

    private void deleteElements(LineMappingStrategy lineStrategy, String[] line, Set<String> keyTypes, Action next) {
        switch (next) {
            case DELETE_CASCADE_ALL:
                keyTypes.forEach(keyType -> deleteElement(lineStrategy, line, keyType, false, true));
                break;
            case DELETE_CASCADE:
                keyTypes.forEach(keyType -> deleteElement(lineStrategy, line, keyType, true, true));
                break;
            case DELETE_NO_CASCADE:
                List<String> sortedKeyTypes = sortKeyTypes(keyTypes);
                sortedKeyTypes.forEach(keyType -> deleteElement(lineStrategy, line, keyType, true, false));
                break;
        }
    }

    @VisibleForTesting
    List<String> sortKeyTypes(Set<String> keyTypes) {
        Set<String> highLevelElements = Sets.newHashSet(keyTypes);
        keyTypes.stream()
                .map(metaSchema::getStrongChildren)
                .forEach(highLevelElements::removeAll);

        ArrayList<String> list = Lists.newArrayList(highLevelElements);
        highLevelElements.stream()
                .flatMap(elt -> metaSchema.getStrongChildren(elt).stream())
                .filter(keyTypes::contains)
                .forEach(list::add);
        return ImmutableList.copyOf(list).reverse();

    }

    private void checkRequiredProperties(ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes) {
        for (Entry<String, Optional<UniqueEntity<Node>>> nodeEntry : nodes.entrySet()) {
            String keyAttribute = nodeEntry.getKey();
            Optional<UniqueEntity<Node>> node = nodeEntry.getValue();
            ImmutableSet<String> requiredProperties = metaSchema.getRequiredProperties(keyAttribute);
            for (String requiredProperty : requiredProperties) {
                node.ifPresent(entity -> {
                    Object value = inferValueFromParent(keyAttribute, requiredProperty, nodes);
                    if (value == null) {
                        throw new IllegalArgumentException(String.format("%s.%s required column is missing. Cannot be inferred from parents neither. Line not imported.", keyAttribute, requiredProperty));
                    } else {
                        entity.entity.setProperty(requiredProperty, value);
                    }
                });
            }
        }
    }

    private Object inferValueFromParent(String keyAttribute, String requiredProperty, ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes) {
        return nodes.getOrDefault(keyAttribute, Optional.empty())
                .flatMap(node -> Optional.ofNullable(node.entity.getProperty(requiredProperty, null)))
                .orElseGet(() -> metaSchema.getRequiredParents(keyAttribute)
                        .map(parentKeyAttribute -> inferValueFromParent(parentKeyAttribute, requiredProperty, nodes))
                        .filter(Objects::nonNull)
                        .findFirst().orElse(null));
    }

    private void createCrossAttributeLinks(String[] line, CsvMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
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

    private void checkCrossAttributeDefinitionExists(CsvMappingStrategy strategy) throws InvalidSchemaException {
        for (MultiElementHeader meHeader : strategy.getMultiElementHeader()) {
            ImmutableSet<String> crossAttributesLinks = metaSchema.getCrossAttributesRelations(meHeader.elementName);
            if (!crossAttributesLinks.contains(meHeader.targetElementName)) {
                throw new InvalidSchemaException("Schema does not allow to create cross attribute between '" +
                        meHeader.elementName + "' and '" + meHeader.targetElementName + "'. Import aborted.");
            }
        }
    }

    private Map<String, Relationship> createCrossAttributeRelationships(Map<String, Optional<UniqueEntity<Node>>> nodes) {
        Map<String, Relationship> multiElementLinks = Maps.newHashMap();
        for (Entry<String, ImmutableSet<String>> rel : metaSchema.getCrossAttributesRelations().entrySet()) {
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

    private ImmutableMultimap<TypedScope, String> createOrUpdatePlanetLink(LineMappingStrategy lineStrategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        ImmutableMultimap.Builder<TypedScope, String> importedElementByScopeBuilder = ImmutableMultimap.builder();
        for (Entry<String, Optional<UniqueEntity<Node>>> node : nodes.entrySet()) {
            if (!node.getValue().isPresent()) continue;
            UniqueEntity<Node> element = node.getValue().get();
            String keyAttribute = node.getKey();

            if (GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(keyAttribute)) continue;

            importedElementByScopeBuilder.put(
                    reviewPlanetElement(lineStrategy, element, nodes),
                    element.entity.getProperty(GraphModelConstants.TAG).toString()
            );
        }
        return importedElementByScopeBuilder.build();
    }

    private TypedScope reviewPlanetElement(LineMappingStrategy lineStrategy,
                                           UniqueEntity<Node> element,
                                           Map<String, Optional<UniqueEntity<Node>>> nodes) {
        String keyType = element.entity.getProperty(_TYPE).toString();

        Optional<Scope> scopeFromImport = lineStrategy.tryToGuessElementScopeInLine(metaSchema, keyType);
        boolean overridable = metaSchema.isOverridable(keyType);

        Scope scopeFromDatabase = topologyLoaderUtils.getScopeFromElementPlanet(element.entity)
                .orElseGet(() -> !overridable ? getScopeFromParent(keyType, nodes).orElse(null) : null);

        return scopeFromImport
                .map(scope -> {
                    if (scopeFromDatabase != null && !scope.tag.equals(scopeFromDatabase.tag)) {
                        // If the imported element scope is different than the existing one
                        // slide the element to the new scope
                        elementScopeSlider.slide(element.entity, scope);
                    }
                    UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scope, element.entity);
                    replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
                    return new TypedScope(scope.tag, keyType);
                })
                .orElseGet(() -> {
                    if (scopeFromDatabase == null) {
                        Object tag = element.entity.getProperty(TAG);
                        throw new IllegalStateException(String.format("Inconsistent element '%s' in db : it is not linked to a planet which is required. Fix this.", tag));
                    } else {
                        // review the planet (in case the element has been created)
                        UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scopeFromDatabase, element.entity);
                        replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
                        return new TypedScope(scopeFromDatabase.tag, keyType);
                    }
                });
    }

    private Optional<Scope> getScopeFromParent(String keyAttribute, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        return metaSchema.getRequiredParent(keyAttribute)
                .flatMap(requiredParent ->
                        nodes.getOrDefault(requiredParent, Optional.empty())
                                .flatMap(nodeUniqueEntity -> topologyLoaderUtils.getScopeFromElementPlanet(nodeUniqueEntity.entity)));
    }

    private void linkToParents(LineMappingStrategy strategy, String keyType, UniqueEntity<Node> keyTypeNode, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-linkToParents").time()) {
            ImmutableList<Relationship> relationships = metaSchema.getParentRelations().get(keyType);
            if (relationships == null || relationships.isEmpty()) {
                return;
            }

            String scopeAttribute = strategy.guessScopeAttributeInLine(metaSchema, keyType);

            for (Relationship relationship : relationships) {
                Node endNode = relationship.getEndNode();
                String toKeytype = endNode.getProperty(GraphModelConstants._TYPE).toString() + GraphModelConstants.KEYTYPE_SEPARATOR +
                        endNode.getProperty(NAME).toString();

                if (metaSchema.isScope(toKeytype) && !scopeAttribute.equals(toKeytype)) {
                    continue;
                }

                Optional<UniqueEntity<Node>> parent = nodes.get(toKeytype);
                if (parent == null || !parent.isPresent()) {
                    String cardinality = relationship.getProperty(GraphModelConstants.CARDINALITY, GraphModelConstants.CARDINALITY_UNIQUE_PARENT).toString();
                    if (keyTypeNode.wasCreated && GraphModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
                        Object tagProperty = keyTypeNode.entity.getProperty(TAG);
                        throw new MissingElementException(String.format("Unable to import '%s' because its required parent of type '%s' is not found. Line is ignored.", tagProperty, toKeytype));
                    } else {
                        continue;
                    }
                }
                createOutgoingUniqueLink(keyTypeNode.entity, parent.get().entity, RelationshipTypes.CONNECT);
            }

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

    private Optional<UniqueEntity<Node>> createElement(LineMappingStrategy lineStrategy, String[] line, String elementKeyType) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createElement").time()) {
            if (GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(elementKeyType)) {
                return Optional.of(UniqueEntity.existing(graphDb.findNode(Labels.SCOPE, "tag", GraphModelConstants.SCOPE_GLOBAL_TAG)));
            }

            Set<String> todelete = metaSchema.getMonoParentRelations(elementKeyType)
                    .filter(lineStrategy.strategy::hasKeyType)
                    .collect(Collectors.toSet());

            int tagIndex = lineStrategy.strategy.getColumnIndex(elementKeyType, GraphModelConstants.TAG);
            if (tagIndex < 0) return Optional.empty();
            String tag = line[tagIndex];
            if (tag.isEmpty()) return Optional.empty();

            boolean isOverridable = metaSchema.isOverridable(elementKeyType);

            UniqueEntity<Node> uniqueEntity;
            if (isOverridable) {
                Scope scope = lineStrategy.guessElementScopeInLine(metaSchema, elementKeyType);
                uniqueEntity = overridableElementFactory.getOrOverride(scope, GraphModelConstants.TAG, tag);
            } else {
                uniqueEntity = networkElementFactory.getOrCreateWithOutcome(GraphModelConstants.TAG, tag);
            }

            Iterable<String> schemasToApply = getSchemasToApply(lineStrategy.strategy, line, elementKeyType);
            boolean isScope = metaSchema.isScope(elementKeyType);
            if (uniqueEntity.wasCreated) {
                if (isScope) {
                    int nameIndex = lineStrategy.strategy.getColumnIndex(elementKeyType, NAME);
                    if (nameIndex < 0) return Optional.empty();
                    String name = line[nameIndex];
                    if (name.isEmpty()) {
                        uniqueEntity.entity.delete();
                        return Optional.empty();
                    }

                    uniqueEntity.entity.addLabel(Labels.SCOPE);
                    if (Iterables.isEmpty(schemasToApply)) {
                        throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + GraphModelConstants.SCHEMA));
                    }
                    applySchemas(elementKeyType, uniqueEntity.entity, schemasToApply);
                }
                uniqueEntity.entity.setProperty(GraphModelConstants._TYPE, elementKeyType);

            } else {
                if (isScope) {
                    if (!Iterables.isEmpty(schemasToApply)) {
                        applySchema(lineStrategy.strategy, line, elementKeyType, tag, uniqueEntity.entity);
                    }
                }
                uniqueEntity.entity.setProperty(GraphModelConstants.UPDATED_AT, Instant.now().toEpochMilli());

                uniqueEntity.entity.getRelationships(Direction.OUTGOING, RelationshipTypes.CONNECT).forEach(r -> {
                    String type = r.getEndNode().getProperty(GraphModelConstants._TYPE, GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE).toString();
                    if (todelete.contains(type)) {
                        r.delete();
                    }
                });
            }
            ImmutableCollection<HeaderElement> elementHeaders = lineStrategy.strategy.getElementHeaders(elementKeyType);
            persistElementProperties(line, elementHeaders, uniqueEntity.entity);

            return Optional.of(uniqueEntity);
        }
    }

    private void deleteElement(LineMappingStrategy lineStrategy, String[] line, String elementKeyType, boolean onlyStrongChild, boolean cascade) {
        int tagIndex = lineStrategy.strategy.getColumnIndex(elementKeyType, GraphModelConstants.TAG);
        if (tagIndex < 0) return;
        String tag = line[tagIndex];
        if (tag.isEmpty()) return;
        Optional<Node> node = Optional.ofNullable(networkElementFactory.getWithOutcome(TAG, tag));
        node.ifPresent(entity -> deleteNetworkElement(entity, elementKeyType, onlyStrongChild, cascade));
    }

    private void deleteNetworkElement(Node entity, String elementKeyType, boolean onlyStrongChild, boolean cascade) {
        Iterable<Relationship> children = entity.getRelationships(INCOMING, RelationshipTypes.CONNECT);
        if (cascade) {
            children.forEach(relationship -> {
                relationship.delete();
                Node startNode = relationship.getStartNode();
                String childType = startNode.getProperty(GraphModelConstants._TYPE, GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE).toString();
                if (!onlyStrongChild || metaSchema.getRequiredParent(childType).map(v -> v.equals(elementKeyType)).orElse(false)) {
                    deleteNetworkElement(startNode, childType, onlyStrongChild, true);
                }
            });
        } else {
            if (children.iterator().hasNext()) {
                String tag = entity.getProperty(TAG).toString();
                throw new IllegalStateException(String.format("Cannot delete %s, its children has not been deleted", tag));
            }
        }
        if (!metaSchema.isScope(elementKeyType)) {
            entity.getRelationships().forEach(Relationship::delete);
            entity.delete();
        }
    }

    private void applySchema(CsvMappingStrategy strategy, String[] line, String elementKeyType, String tag, Node elementNode) {
        try {
            Iterable<String> schemas = getSchemasToApply(strategy, line, elementKeyType);
            applySchemas(elementKeyType, elementNode, schemas);
        } catch (NoSuchElementException e) {
            throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + GraphModelConstants.SCHEMA));
        }
    }

    private Iterable<String> getSchemasToApply(CsvMappingStrategy strategy, String[] line, String elementKeyType) {
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

    private Optional<UniqueEntity<Node>> updateElement(LineMappingStrategy lineStrategy, String[] line, String keyAttribute) throws NoSuchElementException {

        Scope scope = lineStrategy.guessElementScopeInLine(metaSchema, keyAttribute);
        if (!SCOPE_GLOBAL_TAG.equals(scope.tag)) {
            return createElement(lineStrategy, line, keyAttribute);
        }

        ImmutableCollection<HeaderElement> elementHeaders = lineStrategy.strategy.getElementHeaders(keyAttribute);
        HeaderElement tagHeader = elementHeaders.stream()
                .filter(h -> GraphModelConstants.TAG.equals(h.propertyName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(GraphModelConstants.TAG + " not found for element " + keyAttribute + ""));

        String tag = line[tagHeader.index];

        if (tag.isEmpty()) {
            throw new NoSuchElementException("Element " + keyAttribute + " not found in database for update");
        }

        Node node = graphDb.findNode(Labels.NETWORK_ELEMENT, GraphModelConstants.TAG, tag);
        if (node != null) {
            persistElementProperties(line, elementHeaders, node);
            return Optional.of(UniqueEntity.existing(node));
        } else {
            throw new NoSuchElementException("Element with tag " + tag + " not found in database for update");
        }
    }

    private void persistElementProperties(String[] line, ImmutableCollection<HeaderElement> elementHeaders, Node elementNode) {
        elementHeaders.stream()
                .filter(h -> !RESERVED_PROPERTIES.contains(h.propertyName))
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
