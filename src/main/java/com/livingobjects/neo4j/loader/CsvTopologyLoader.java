package com.livingobjects.neo4j.loader;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;
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
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvValidationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.livingobjects.neo4j.helper.RelationshipUtils.replaceRelationships;
import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.RESERVED_PROPERTIES;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.APPLIED_TO;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.CROSS_ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.EXTEND;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class CsvTopologyLoader {

    private static final int MAX_TRANSACTION_COUNT = 500;
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvTopologyLoader.class);
    private static final String KEEP_VALUE_TOKEN = "#KEEP_VALUE";

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final TemplatedPlanetFactory planetFactory;
    private final UniqueElementFactory networkElementFactory;
    private final OverridableElementFactory overridableElementFactory;
    private final ElementScopeSlider elementScopeSlider;
    private final TransactionManager txManager;
    private final TopologyLoaderUtils topologyLoaderUtils;
    private final MetaSchema metaSchema;

    public CsvTopologyLoader(GraphDatabaseService graphDb, MetricRegistry metrics) {
        this.metrics = metrics;
        this.graphDb = graphDb;
        this.txManager = new TransactionManager(graphDb);

        this.networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);
        UniqueElementFactory scopeElementFactory = new UniqueElementFactory(graphDb, Labels.SCOPE, Optional.empty());
        this.overridableElementFactory = OverridableElementFactory.networkElementFactory(graphDb);

        this.planetFactory = new TemplatedPlanetFactory(graphDb);
        this.elementScopeSlider = new ElementScopeSlider(planetFactory);

        topologyLoaderUtils = new TopologyLoaderUtils(scopeElementFactory);

        try (Transaction tx = graphDb.beginTx()) {
            this.metaSchema = new MetaSchema(tx);
        }
    }

    public Neo4jLoadResult loadFromStream(InputStream is) throws IOException, CsvValidationException {
        CSVReader reader = new CSVReaderBuilder(new InputStreamReader(is))
                .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                .build();

        String[] nextLine;

        int lineIndex = 0;
        int imported = 0;
        Map<TypedScope, Set<String>> importedElementByScope = Maps.newHashMap();
        List<String[]> currentTransaction = Lists.newArrayListWithCapacity(MAX_TRANSACTION_COUNT);
        Map<Integer, String> errors = Maps.newHashMap();

        Transaction tx = graphDb.beginTx();
        try {
            CsvMappingStrategy strategy = CsvMappingStrategy.captureHeader(reader, metaSchema);
            checkKeyAttributesExist(strategy);
            checkCrossAttributeDefinitionExists(strategy);
            while ((nextLine = reader.readNext()) != null) {
                try {
                    ImmutableSet<String> scopeKeyTypes = strategy.guessKeyTypesForLine(metaSchema.getScopeTypes(), nextLine);
                    ImmutableMultimap<TypedScope, String> importedElementByScopeInLine = importLine(nextLine, scopeKeyTypes, strategy, tx);
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
                    LOGGER.debug(e.getLocalizedMessage());
                    LOGGER.debug(Arrays.toString(nextLine));
                    tx = renewTransaction(strategy, currentTransaction, tx);
                    errors.put(lineIndex, e.getMessage());
                } catch (Exception e) {
                    LOGGER.error("error", e);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("STACKTRACE", e);
                        LOGGER.debug(Arrays.toString(nextLine));
                    }
                    tx = renewTransaction(strategy, currentTransaction, tx);
                    errors.put(lineIndex, e.getMessage());
                }
                lineIndex++;
            }
            tx.commit();
        } finally {
            tx.close();
        }

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
        return txManager.properlyRenewTransaction(tx, currentTransaction, (ct, transaction) -> {
            ImmutableSet<String> scopeKeyTypes = strategy.guessKeyTypesForLine(metaSchema.getScopeTypes(), ct);
            importLine(ct, scopeKeyTypes, strategy, transaction);
        });
    }

    private ImmutableMultimap<TypedScope, String> importLine(String[] line, ImmutableSet<String> scopeKeytypes, CsvMappingStrategy strategy,
                                                             Transaction tx) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {
            ImmutableMap<String, Set<String>> lineage = strategy.guessElementCreationStrategy(scopeKeytypes, tx);
            LineMappingStrategy lineStrategy = new LineMappingStrategy(metaSchema, strategy, line);

            Map<String, Action> markedToDelete = strategy.getAllElementsType().stream()
                    .map(keyType -> lineStrategy.getValue(keyType, Action.STATUS_HEADER)
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
                                .flatMap(elt -> Stream.concat(Stream.of(elt), metaSchema.getAllChildren(tx, elt).stream()))
                                .collect(Collectors.toSet());
                        break;
                    case DELETE_CASCADE:
                        allElementToDeleteBld = markedToDelete.keySet().stream()
                                .flatMap(elt -> Stream.concat(Stream.of(elt), metaSchema.getStrongChildren(tx, elt).stream()))
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
                    .map(key -> Maps.immutableEntry(key, updateElement(lineStrategy, key, tx)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            // Create elements
            Map<String, Optional<UniqueEntity<Node>>> elementsWithParents = lineage.keySet().stream()
                    .filter(key -> !allElementToDelete.contains(key))
                    .map(key -> Maps.immutableEntry(key, createElement(lineStrategy, key, tx)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            Map<String, Optional<UniqueEntity<Node>>> nodesBuilder = Maps.newHashMap();
            nodesBuilder.putAll(elementsWithoutParents);
            nodesBuilder.putAll(elementsWithParents);
            markedToDelete.keySet().forEach(keyType -> nodesBuilder.put(keyType, Optional.empty()));
            nodesBuilder.put(SCOPE_GLOBAL_ATTRIBUTE, Optional.of(UniqueEntity.existing(metaSchema.getTheGlobalScopeNode(tx))));

            ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes = ImmutableMap.copyOf(nodesBuilder);

            createCrossAttributeLinks(lineStrategy, nodes);

            createConnectLink(lineStrategy, nodes, tx);

            checkRequiredProperties(nodes, lineStrategy, tx);

            if (!markedToDelete.isEmpty()) {
                deleteElements(lineStrategy, markedToDelete.keySet(), markedToDelete.values().iterator().next(), tx); // We have checked that only one distinct action is present
            }

            return createOrUpdatePlanetLink(lineStrategy, nodes, tx);
        }
    }

    private void deleteElements(LineMappingStrategy line, Set<String> keyTypes, Action action,
                                Transaction tx) {
        switch (action) {
            case DELETE_CASCADE_ALL:
            case DELETE_CASCADE:
                keyTypes.forEach(keyType -> deleteElement(line, keyType, action, tx));
                break;
            case DELETE_NO_CASCADE:
                List<String> sortedKeyTypes = sortKeyTypes(keyTypes, tx);
                sortedKeyTypes.forEach(keyType -> deleteElement(line, keyType, action, tx));
                break;
        }
    }

    @VisibleForTesting
    List<String> sortKeyTypes(Set<String> keyTypes, Transaction tx) {
        Set<String> highLevelElements = Sets.newHashSet(keyTypes);
        keyTypes.stream()
                .map(keyType -> metaSchema.getStrongChildren(tx, keyType))
                .forEach(highLevelElements::removeAll);

        ArrayList<String> list = Lists.newArrayList(highLevelElements);
        highLevelElements.stream()
                .flatMap(elt -> metaSchema.getStrongChildren(tx, elt).stream())
                .filter(keyTypes::contains)
                .forEach(list::add);
        return ImmutableList.copyOf(list).reverse();

    }

    private void checkRequiredProperties(ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes,
                                         LineMappingStrategy line,
                                         Transaction tx) {
        for (Entry<String, Optional<UniqueEntity<Node>>> nodeEntry : nodes.entrySet()) {
            String keyAttribute = nodeEntry.getKey();
            Optional<UniqueEntity<Node>> node = nodeEntry.getValue();
            ImmutableSet<String> requiredProperties = metaSchema.getRequiredProperties(keyAttribute);
            for (String requiredProperty : requiredProperties) {
                node.ifPresent(entity -> {
                    Object value = inferFromLine(keyAttribute, requiredProperty, line, tx)
                            .map(Object.class::cast)
                            .orElseGet(() -> inferValueFromParent(keyAttribute, requiredProperty, nodes, tx)
                                    .orElseThrow(() -> new IllegalArgumentException(String.format("%s.%s required column is missing. Cannot be inferred from parents neither. Line not imported.", keyAttribute, requiredProperty))));

                    entity.entity.setProperty(requiredProperty, value);
                });
            }
        }
    }

    private Optional<String> inferFromLine(String keyAttribute, String requiredProperty, LineMappingStrategy line, Transaction tx) {
        Optional<String> maybeValue = line.getValue(keyAttribute, requiredProperty);
        if (maybeValue.isEmpty()) {
            maybeValue = metaSchema.getRequiredParents(tx, keyAttribute)
                    .map(parentKeyAttribute -> inferFromLine(parentKeyAttribute, requiredProperty, line, tx).orElse(null))
                    .filter(Objects::nonNull)
                    .findFirst();
        }
        return maybeValue;
    }


    private Optional<Object> inferValueFromParent(String keyAttribute, String requiredProperty, ImmutableMap<String, Optional<UniqueEntity<Node>>> nodes, Transaction tx) {
        Optional<Object> maybeValue = nodes.getOrDefault(keyAttribute, Optional.empty())
                .flatMap(node -> Optional.ofNullable(node.entity.getProperty(requiredProperty, null)));
        if (maybeValue.isEmpty()) {
            maybeValue = metaSchema.getRequiredParents(tx, keyAttribute)
                    .map(parentKeyAttribute -> inferValueFromParent(parentKeyAttribute, requiredProperty, nodes, tx).orElse(null))
                    .filter(Objects::nonNull)
                    .findFirst();
        }
        return maybeValue;
    }

    private void createCrossAttributeLinks(LineMappingStrategy line, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createCrossAttributeLinks").time()) {
            Map<String, Relationship> crossAttributeRelationships = createCrossAttributeRelationships(nodes);
            for (MultiElementHeader meHeader : line.strategy.getMultiElementHeader()) {
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
        for (Entry<String, ImmutableSet<Tuple2<String, String>>> rel : metaSchema.getCrossAttributesRelations().entrySet()) {
            String keyType = rel.getKey();
            Optional<UniqueEntity<Node>> startNode = nodes.getOrDefault(keyType, Optional.empty());
            startNode.ifPresent(fromNode -> {
                for (Tuple2<String, String> endKeyType : rel.getValue()) {
                    if (!keyType.equals(endKeyType._1())) {
                        Optional<UniqueEntity<Node>> optEndNode = nodes.getOrDefault(endKeyType._1(), Optional.empty());
                        optEndNode.ifPresent(toNode -> {
                            Relationship link = createOutgoingUniqueLink(fromNode.entity, toNode.entity, RelationshipTypes.CROSS_ATTRIBUTE, endKeyType._2());
                            String key = keyType + ELEMENT_SEPARATOR + endKeyType._1();
                            multiElementLinks.put(key, link);
                        });
                    }
                }
            });
        }
        return multiElementLinks;
    }

    private void createConnectLink(LineMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes, Transaction tx) {
        nodes.forEach((keyType, oNode) ->
                oNode.ifPresent(node -> linkToParents(strategy, keyType, node, nodes, tx)));
    }

    private ImmutableMultimap<TypedScope, String> createOrUpdatePlanetLink(LineMappingStrategy lineStrategy,
                                                                           Map<String, Optional<UniqueEntity<Node>>> nodes,
                                                                           Transaction tx) {
        ImmutableMultimap.Builder<TypedScope, String> importedElementByScopeBuilder = ImmutableMultimap.builder();
        for (Entry<String, Optional<UniqueEntity<Node>>> node : nodes.entrySet()) {
            if (node.getValue().isEmpty()) continue;
            UniqueEntity<Node> element = node.getValue().get();
            String keyAttribute = node.getKey();

            if (GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(keyAttribute)) continue;

            importedElementByScopeBuilder.put(
                    reviewPlanetElement(lineStrategy, element, nodes, tx),
                    element.entity.getProperty(GraphModelConstants.TAG).toString()
            );
        }
        return importedElementByScopeBuilder.build();
    }

    private TypedScope reviewPlanetElement(LineMappingStrategy lineStrategy,
                                           UniqueEntity<Node> element,
                                           Map<String, Optional<UniqueEntity<Node>>> nodes,
                                           Transaction tx) {
        String keyType = element.entity.getProperty(_TYPE).toString();

        Optional<Scope> scopeFromImport = lineStrategy.tryToGuessElementScopeInLine(keyType, tx);
        boolean overridable = metaSchema.isOverridable(keyType);

        Scope scopeFromDatabase = topologyLoaderUtils.getScopeFromElementPlanet(element.entity, tx)
                .orElseGet(() -> !overridable ? getScopeFromParent(keyType, nodes,tx).orElse(null) : null);

        return scopeFromImport
                .map(scope -> {
                    if (scopeFromDatabase != null && !scope.tag.equals(scopeFromDatabase.tag)) {
                        // If the imported element scope is different than the existing one
                        // slide the element to the new scope
                        elementScopeSlider.slide(element.entity, scope, tx);
                    }
                    UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scope, element.entity, tx);
                    replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
                    return new TypedScope(scope.tag, keyType);
                })
                .orElseGet(() -> {
                    if (scopeFromDatabase == null) {
                        Object tag = element.entity.getProperty(TAG);
                        throw new IllegalStateException(String.format("Inconsistent element '%s' in db : it is not linked to a planet which is required. Fix this.", tag));
                    } else {
                        // review the planet (in case the element has been created)
                        UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scopeFromDatabase, element.entity, tx);
                        replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
                        return new TypedScope(scopeFromDatabase.tag, keyType);
                    }
                });
    }

    private Optional<Scope> getScopeFromParent(String keyAttribute, Map<String, Optional<UniqueEntity<Node>>> nodes,
                                               Transaction tx) {
        return metaSchema.getRequiredParent(tx, keyAttribute)
                .flatMap(requiredParent ->
                        nodes.getOrDefault(requiredParent, Optional.empty())
                                .flatMap(nodeUniqueEntity -> topologyLoaderUtils.getScopeFromElementPlanet(nodeUniqueEntity.entity, tx)));
    }

    private void linkToParents(LineMappingStrategy strategy,
                               String keyType,
                               UniqueEntity<Node> keyTypeNode,
                               Map<String, Optional<UniqueEntity<Node>>> nodes,
                               Transaction tx) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-linkToParents").time()) {
            ImmutableList<Relationship> relationships = metaSchema.getParentRelations(tx).get(keyType);
            if (relationships == null || relationships.isEmpty()) {
                return;
            }

            String scopeAttribute = strategy.guessScopeAttributeInLine(keyType, tx);

            for (Relationship relationship : relationships) {
                Node endNode = relationship.getEndNode();
                String toKeytype = endNode.getProperty(GraphModelConstants._TYPE).toString() + GraphModelConstants.KEYTYPE_SEPARATOR +
                        endNode.getProperty(NAME).toString();

                if (metaSchema.isScope(toKeytype) && !scopeAttribute.equals(toKeytype)) {
                    continue;
                }

                Optional<UniqueEntity<Node>> parent = nodes.getOrDefault(toKeytype, Optional.empty());
                if (parent.isEmpty()) {
                    String cardinality = relationship.getProperty(GraphModelConstants.CARDINALITY, GraphModelConstants.CARDINALITY_UNIQUE_PARENT).toString();
                    if (keyTypeNode.wasCreated && GraphModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
                        Object tagProperty = keyTypeNode.entity.getProperty(TAG);
                        throw new MissingElementException(String.format("Unable to import '%s' because its required parent of type '%s' is not found. Line is ignored.", tagProperty, toKeytype));
                    } else {
                        continue;
                    }
                }
                createOutgoingUniqueLink(keyTypeNode.entity, parent.get().entity, RelationshipTypes.CONNECT, null);
            }
        }
    }

    private Relationship createOutgoingUniqueLink(Node node,
                                                  Node parent,
                                                  org.neo4j.graphdb.RelationshipType linkType,
                                                  String typeAttr) {
        for (Relationship next : node.getRelationships(Direction.OUTGOING, linkType)) {
            if (next.getEndNode().equals(parent)) {
                return next;
            }
        }
        Relationship relationship = node.createRelationshipTo(parent, linkType);
        if (typeAttr != null) {
            relationship.setProperty(CsvLoaderHelper.TYPE_ATTR, typeAttr);
        }
        return relationship;
    }

    private Optional<UniqueEntity<Node>> createElement(LineMappingStrategy line, String elementKeyType,
                                                       Transaction tx) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createElement").time()) {
            if (GraphModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(elementKeyType)) {
                return Optional.of(UniqueEntity.existing(tx.findNode(Labels.SCOPE, "tag", GraphModelConstants.SCOPE_GLOBAL_TAG)));
            }

            Set<String> todelete = metaSchema.getMonoParentRelations(tx, elementKeyType)
                    .filter(line.strategy::hasKeyType)
                    .collect(Collectors.toSet());

            Optional<String> tagValue = line.getValue(elementKeyType, TAG);
            if (tagValue.isEmpty()) {
                return Optional.empty();
            }
            String tag = tagValue.get();

            boolean isOverridable = metaSchema.isOverridable(elementKeyType);

            UniqueEntity<Node> uniqueEntity;
            if (isOverridable) {
                Scope scope = line.guessElementScopeInLine(elementKeyType, tx);
                uniqueEntity = overridableElementFactory.getOrOverride(scope, GraphModelConstants.TAG, tag, tx);
            } else {
                uniqueEntity = networkElementFactory.getOrCreateWithOutcome(GraphModelConstants.TAG, tag, tx);
            }

            Iterable<String> schemasToApply = getSchemasToApply(line, elementKeyType);
            boolean isScope = metaSchema.isScope(elementKeyType);
            if (uniqueEntity.wasCreated) {
                if (isScope) {
                    int nameIndex = line.strategy.getColumnIndex(elementKeyType, NAME);
                    if (nameIndex < 0) return Optional.empty();
                    Optional<String> name = line.getValue(nameIndex);
                    if (name.isEmpty()) {
                        uniqueEntity.entity.delete();
                        return Optional.empty();
                    }

                    uniqueEntity.entity.addLabel(Labels.SCOPE);
                    if (Iterables.isEmpty(schemasToApply)) {
                        throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + GraphModelConstants.SCHEMA));
                    }
                    applySchemas(elementKeyType, uniqueEntity.entity, schemasToApply, tx);
                }
                uniqueEntity.entity.setProperty(GraphModelConstants._TYPE, elementKeyType);

            } else {
                if (isScope) {
                    if (!Iterables.isEmpty(schemasToApply)) {
                        applySchema(line, elementKeyType, tag, uniqueEntity.entity, tx);
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
            ImmutableCollection<HeaderElement> elementHeaders = line.strategy.getElementHeaders(elementKeyType);
            persistElementProperties(line, elementHeaders, uniqueEntity.entity);

            return Optional.of(uniqueEntity);
        }
    }

    private void deleteElement(LineMappingStrategy line, String elementKeyType, Action action,
                               Transaction tx) {
        line.getValue(elementKeyType, TAG)
                .map(tag -> networkElementFactory.getWithOutcome(TAG, tag, tx))
                .ifPresent(entity -> deleteElementRecursive(entity, elementKeyType, action, Sets.newConcurrentHashSet(), tx));
    }

    private void deleteElementRecursive(Node entity, String elementKeyType, Action action, Set<Long> deleted, Transaction tx) {
        Node planetEntity = entity.getSingleRelationship(ATTRIBUTE, OUTGOING).getEndNode();
        String elementScope = planetEntity.getProperty(SCOPE).toString();
        entity.getRelationships(INCOMING, RelationshipTypes.CONNECT).forEach(relationship -> {
            Node child = relationship.getStartNode();
            if (deleted.contains(child.getId())) {
                return;
            }
            Node childPlanetEntity = child.getSingleRelationship(ATTRIBUTE, OUTGOING).getEndNode();
            String childScope = childPlanetEntity.getProperty(SCOPE).toString();
            String childType = child.getProperty(GraphModelConstants._TYPE).toString();
            if (metaSchema.getRequiredParent(tx, childType).map(v -> v.equals(elementKeyType)).orElse(false)) {
                // if strong child
                switch (action) {
                    case DELETE_CASCADE_ALL:
                    case DELETE_CASCADE:
                        relationship.delete();
                        if (metaSchema.scopeLevel(childScope) > metaSchema.scopeLevel(elementScope)) {
                            // Do not delete elements of higher scope
                            return;
                        }
                        deleteElementRecursive(child, childType, action, deleted, tx);
                        break;
                    case DELETE_NO_CASCADE:
                        String tag = entity.getProperty(TAG).toString();
                        throw new IllegalStateException(String.format("Cannot delete %s, its children has not been deleted", tag));

                }
            } else {
                // if not strong child
                relationship.delete();
                if (action == Action.DELETE_CASCADE_ALL) { // Only CASCADE_ALL delete not strong child, the other modes will just delete relationship
                    if (metaSchema.scopeLevel(childScope) > metaSchema.scopeLevel(elementScope)) {
                        // Do not delete elements of higher scope
                        return;
                    }
                    deleteElementRecursive(child, childType, action, deleted, tx);
                }
            }
        });

        if (!metaSchema.isScope(elementKeyType)) {
            entity.getRelationships(INCOMING, EXTEND).forEach(relationship -> {
                Node startNode = relationship.getStartNode();
                String childType = startNode.getProperty(GraphModelConstants._TYPE).toString();
                deleteElementRecursive(relationship.getStartNode(), childType, action, deleted, tx);

            });
            entity.getRelationships(OUTGOING, RelationshipTypes.CONNECT).forEach(Relationship::delete);
            entity.getRelationships(OUTGOING, ATTRIBUTE).forEach(Relationship::delete);
            entity.getRelationships(OUTGOING, EXTEND).forEach(Relationship::delete);
            entity.getRelationships(CROSS_ATTRIBUTE).forEach(Relationship::delete);
            entity.delete();
            deleted.add(entity.getId());
        }
    }

    private void applySchema(LineMappingStrategy line, String elementKeyType, String tag, Node elementNode, Transaction tx) {
        try {
            Iterable<String> schemas = getSchemasToApply(line, elementKeyType);
            applySchemas(elementKeyType, elementNode, schemas, tx);
        } catch (NoSuchElementException e) {
            throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + GraphModelConstants.SCHEMA));
        }
    }

    private Iterable<String> getSchemasToApply(LineMappingStrategy line, String elementKeyType) {
        return line.getValue(elementKeyType, GraphModelConstants.SCHEMA)
                .map(val -> Splitter.on(',').omitEmptyStrings().trimResults().split(val))
                .orElse(ImmutableSet.of());
    }

    private void applySchemas(String elementKeyType, Node elementNode, Iterable<String> schemas, Transaction tx) {
        Set<Node> schemaNodes = Sets.newHashSet();
        schemas.forEach(schema -> {
            Node schemaNode = tx.findNode(Labels.SCHEMA, GraphModelConstants.ID, schema);
            if (schemaNode != null) {
                schemaNodes.add(schemaNode);
            } else {
                throw new InvalidScopeException(String.format("Unable to apply schema '%s' for node '%s'. Schema not found.", schema, elementKeyType));
            }
        });
        RelationshipUtils.updateRelationships(INCOMING, elementNode, APPLIED_TO, schemaNodes);
    }

    private Optional<UniqueEntity<Node>> updateElement(LineMappingStrategy line, String keyAttribute,
                                                       Transaction tx) throws NoSuchElementException {

        Scope scope = line.guessElementScopeInLine(keyAttribute, tx);
        if (!SCOPE_GLOBAL_TAG.equals(scope.tag)) {
            return createElement(line, keyAttribute, tx);
        }

        ImmutableCollection<HeaderElement> elementHeaders = line.strategy.getElementHeaders(keyAttribute);
        HeaderElement tagHeader = elementHeaders.stream()
                .filter(h -> GraphModelConstants.TAG.equals(h.propertyName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(GraphModelConstants.TAG + " not found for element " + keyAttribute + ""));

        String tag = line.getValue(tagHeader.index)
                .orElseThrow(() -> new NoSuchElementException("Element " + keyAttribute + " not found in database for update"));

        Node node = tx.findNode(Labels.NETWORK_ELEMENT, GraphModelConstants.TAG, tag);
        if (node != null) {
            persistElementProperties(line, elementHeaders, node);
            return Optional.of(UniqueEntity.existing(node));
        } else {
            throw new NoSuchElementException("Element with tag " + tag + " not found in database for update");
        }
    }

    private void persistElementProperties(LineMappingStrategy line, ImmutableCollection<HeaderElement> elementHeaders, Node elementNode) {
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

    private static <T extends Entity> void persistElementProperty(HeaderElement header, LineMappingStrategy line, T elementNode) {
        Object value = line.getValue(header.index)
                .map(field -> PropertyConverter.convert(field, header.type, header.isArray))
                .orElse(null);

        if (value != null) {
            if (!value.equals(KEEP_VALUE_TOKEN)) {
                elementNode.setProperty(header.propertyName, value);
            }
        } else {
            elementNode.removeProperty(header.propertyName);
        }
    }
}
