package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
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
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.result.Neo4jLoadResult;
import com.livingobjects.neo4j.model.result.TypedScope;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.helper.RelationshipUtils.replaceRelationships;
import static com.livingobjects.neo4j.model.header.HeaderElement.ELEMENT_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.SCOPE_CLASS;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.SCOPE_GLOBAL_TAG;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._OVERRIDABLE;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.APPLIED_TO;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class IWanTopologyLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(IWanTopologyLoader.class);
    private static final int MAX_TRANSACTION_COUNT = 500;

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final TemplatedPlanetFactory planetFactory;
    private final UniqueElementFactory networkElementFactory;
    private final OverridableElementFactory overridableElementFactory;
    private final ElementScopeSlider elementScopeSlider;
    private final TransactionManager txManager;

    private final Node theGlobalNode;
    private final ImmutableSet<String> overridableType;
    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    private final ImmutableMap<String, ImmutableSet<String>> crossAttributesRelations;
    private final ImmutableSet<String> scopeTypes;
    private final ImmutableMap<String, String> scopeByKeyTypes;

    public IWanTopologyLoader(GraphDatabaseService graphDb, MetricRegistry metrics) {
        this.metrics = metrics;
        this.graphDb = graphDb;
        this.txManager = new TransactionManager(graphDb);
        try (Transaction ignore = graphDb.beginTx()) {

            networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);
            overridableElementFactory = OverridableElementFactory.networkElementFactory(graphDb);

            this.theGlobalNode = graphDb.findNode(Labels.SCOPE, TAG, GLOBAL_SCOPE.tag);
            Objects.requireNonNull(this.theGlobalNode, "Global Scope node not found in database !");

            ImmutableMap.Builder<String, Node> importableKeyTypesBldr = ImmutableMap.builder();
            ImmutableSet.Builder<String> overrideBldr = ImmutableSet.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableSet<String>> crossAttributesRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<Node, String> scopesBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, String> scopeByKeyTypesBldr = ImmutableMap.builder();
            graphDb.findNodes(Labels.ATTRIBUTE).forEachRemaining(n -> {
                String keytype = n.getProperty(IwanModelConstants._TYPE).toString();
                String key = keytype + IwanModelConstants.KEYTYPE_SEPARATOR + n.getProperty(NAME).toString();
                boolean isOverride = (boolean) n.getProperty(_OVERRIDABLE, false);
                if (isOverride) overrideBldr.add(key);

                if (IwanModelConstants.KEY_TYPES.contains(keytype)) {
                    if (IwanModelConstants.IMPORTABLE_KEY_TYPES.contains(keytype)) {
                        importableKeyTypesBldr.put(key, n);
                    }
                    ImmutableList.Builder<Relationship> crels = ImmutableList.builder();
                    ImmutableList.Builder<Relationship> prels = ImmutableList.builder();
                    n.getRelationships(INCOMING, RelationshipTypes.PARENT).forEach(crels::add);
                    n.getRelationships(Direction.OUTGOING, RelationshipTypes.PARENT).forEach(prels::add);
                    ImmutableSet<String> crossAttributes = IWanLoaderHelper.getCrossAttributes(n);
                    if (!IwanModelConstants.LABEL_TYPE.equals(keytype) && prels.build().isEmpty()) {
                        scopesBldr.put(n, key);
                    }
                    childrenRelationsBldr.put(key, crels.build());
                    parentRelationsBldr.put(key, prels.build());
                    crossAttributesRelationsBldr.put(key, crossAttributes);
                }
            });

            this.overridableType = overrideBldr.build();
            this.childrenRelations = childrenRelationsBldr.build();
            this.parentRelations = parentRelationsBldr.build();
            this.crossAttributesRelations = crossAttributesRelationsBldr.build();

            this.planetFactory = new TemplatedPlanetFactory(graphDb);
            this.elementScopeSlider = new ElementScopeSlider(planetFactory);

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
        return IWanLoaderHelper.getParent(attributeNode)
                .map(node -> getScopeContext(scopes, node))
                .orElseGet(() -> Optional.ofNullable(scopes.get(attributeNode)));
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
                LineMappingStrategy lineStrategy = strategy.reduceStrategyForLine(ImmutableSet.copyOf(scopeByKeyTypes.values()), nextLine);
                ImmutableSet<String> scopeKeyTypes = lineStrategy.guessKeyTypesForLine(scopeTypes, nextLine);
                ImmutableMultimap<TypedScope, String> importedElementByScopeInLine = importLine(nextLine, scopeKeyTypes, lineStrategy);
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

    private Transaction renewTransaction(IwanMappingStrategy strategy, List<String[]> currentTransaction, Transaction tx) {
        tx = txManager.properlyRenewTransaction(tx, currentTransaction, ct -> {
            LineMappingStrategy lineStrategy = strategy.reduceStrategyForLine(ImmutableSet.copyOf(scopeByKeyTypes.values()), ct);
            ImmutableSet<String> scopeKeyTypes = lineStrategy.guessKeyTypesForLine(scopeTypes, ct);
            importLine(ct, scopeKeyTypes, lineStrategy);
        });
        return tx;
    }

    private ImmutableMultimap<TypedScope, String> importLine(String[] line, ImmutableSet<String> scopeKeytypes, LineMappingStrategy strategy) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {
            ImmutableMap<String, Set<String>> lineage = strategy.guessElementCreationStrategy(scopeKeytypes, childrenRelations);

            // Try to update elements which are not possible to create (no parent founds)
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

    private ImmutableMultimap<TypedScope, String> createPlanetLink(LineMappingStrategy strategy, Map<String, Optional<UniqueEntity<Node>>> nodes) {
        ImmutableMultimap.Builder<TypedScope, String> importedElementByScopeBuilder = ImmutableMultimap.builder();
        for (Entry<String, Optional<UniqueEntity<Node>>> node : nodes.entrySet()) {
            if (!node.getValue().isPresent()) continue;
            UniqueEntity<Node> element = node.getValue().get();
            String keyType = node.getKey();

            if (IwanModelConstants.SCOPE_GLOBAL_ATTRIBUTE.equals(keyType)) continue;

            importedElementByScopeBuilder.put(
                    reviewPlanetElement(strategy, element),
                    element.entity.getProperty(IwanModelConstants.TAG).toString()
            );
        }
        return importedElementByScopeBuilder.build();
    }

    private TypedScope reviewPlanetElement(LineMappingStrategy strategy, UniqueEntity<Node> element) {
        String keyType = element.entity.getProperty(_TYPE).toString();
        boolean isOverridable = overridableType.contains(keyType);
        boolean isGlobal = Optional.ofNullable(scopeByKeyTypes.get(keyType))
                .map(SCOPE_GLOBAL_ATTRIBUTE::equals)
                .orElse(false);
        Scope scopeFromImport = IWanLoaderHelper.consolidateScope(strategy.scope, isOverridable, isGlobal);

        String existingScopeTag = getElementScopeInDatabase(element).orElse(null);

        if (scopeFromImport != null) {
            if (existingScopeTag != null && !scopeFromImport.tag.equals(existingScopeTag)) {
                // If the imported element scope is different than the existing one
                // slide the element to the new scope
                elementScopeSlider.slide(element.entity, scopeFromImport);
            }
            UniqueEntity<Node> planet = planetFactory.localizePlanetForElement(scopeFromImport, element.entity);
            replaceRelationships(OUTGOING, element.entity, ATTRIBUTE, ImmutableSet.of(planet.entity));
            return new TypedScope(scopeFromImport.tag, keyType);
        } else {
            if (existingScopeTag == null) {
                Object tag = element.entity.getProperty(TAG);
                throw new IllegalStateException(String.format("Inconsistent element '%s' in db : it is not linked to a planet which is required. Fix this.", tag));
            }
            return new TypedScope(existingScopeTag, keyType);
        }
    }

    private Optional<String> getElementScopeInDatabase(UniqueEntity<Node> element) {
        Iterator<Relationship> iterator = element.entity.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING).iterator();
        if (iterator.hasNext()) {
            Relationship attributeRelationship = iterator.next();
            Node planetNode = attributeRelationship.getEndNode();
            String scopeTag = planetNode.getProperty(SCOPE, "").toString();
            if (Strings.isNullOrEmpty(scopeTag)) {
                Object tag = element.entity.getProperty(TAG);
                Object planetName = planetNode.getProperty(NAME);
                throw new IllegalStateException(String.format("Inconsistent element '%s' in db : its planet '%s' does not have a scope property which is required. Fix this.", tag, planetName));
            }
            return Optional.of(scopeTag);
        } else {
            return Optional.empty();
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
                        endNode.getProperty(NAME).toString();
                if (isOverride && scopeTypes.contains(toKeytype)) {
                    toKeytype = Optional.ofNullable(strategy.scope).orElse(GLOBAL_SCOPE).attribute;
                }

                Optional<UniqueEntity<Node>> parent = nodes.get(toKeytype);
                if (parent == null || !parent.isPresent()) {
                    String cardinality = relationship.getProperty(IwanModelConstants.CARDINALITY, IwanModelConstants.CARDINALITY_UNIQUE_PARENT).toString();
                    if (keyTypeNode.wasCreated && IwanModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality)) {
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
                    .map(r -> r.getEndNode().getProperty(IwanModelConstants._TYPE).toString() + IwanModelConstants.KEYTYPE_SEPARATOR + r.getEndNode().getProperty(NAME).toString())
                    .filter(strategy::hasKeyType)
                    .collect(Collectors.toSet());

            int tagIndex = strategy.getColumnIndex(elementKeyType, IwanModelConstants.TAG);
            if (tagIndex < 0) return Optional.empty();
            String tag = line[tagIndex];
            if (tag.isEmpty()) return Optional.empty();

            boolean isScope = scopeTypes.contains(elementKeyType);
            UniqueEntity<Node> uniqueEntity = (isOverridable) ?
                    overridableElementFactory.getOrOverride(strategy.scope, IwanModelConstants.TAG, tag) :
                    networkElementFactory.getOrCreateWithOutcome(IwanModelConstants.TAG, tag);
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
                        throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + IwanModelConstants.SCHEMA));
                    }
                    applySchemas(elementKeyType, elementNode, schemasToApply);
                }
                elementNode.setProperty(IwanModelConstants._TYPE, elementKeyType);

            } else {
                if (isScope) {
                    if (!Iterables.isEmpty(schemasToApply)) {
                        applySchema(strategy, line, elementKeyType, tag, elementNode);
                    }
                }
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
            Iterable<String> schemas = getSchemasToApply(strategy, line, elementKeyType);
            applySchemas(elementKeyType, elementNode, schemas);
        } catch (NoSuchElementException e) {
            throw new InvalidScopeException(String.format("Unable to apply schema for '%s'. Column '%s' not found.", tag, elementKeyType + '.' + IwanModelConstants.SCHEMA));
        }
    }

    private Iterable<String> getSchemasToApply(IwanMappingStrategy strategy, String[] line, String elementKeyType) {
        return strategy.tryColumnIndex(elementKeyType, IwanModelConstants.SCHEMA)
                .map(schemaIndex -> Splitter.on(',').omitEmptyStrings().trimResults().split(line[schemaIndex]))
                .orElse(ImmutableSet.of());
    }

    private void applySchemas(String elementKeyType, Node elementNode, Iterable<String> schemas) {
        Set<Node> schemaNodes = Sets.newHashSet();
        schemas.forEach(schema -> {
            Node schemaNode = graphDb.findNode(Labels.SCHEMA, IwanModelConstants.ID, schema);
            if (schemaNode != null) {
                schemaNodes.add(schemaNode);
            } else {
                throw new InvalidScopeException(String.format("Unable to apply schema '%s' for node '%s'. Schema not found.", schema, elementKeyType));
            }
        });
        RelationshipUtils.updateRelationships(INCOMING, elementNode, APPLIED_TO, schemaNodes);
    }

    private Optional<UniqueEntity<Node>> updateElement(LineMappingStrategy strategy, String[] line, String elementName) throws NoSuchElementException {
        boolean isOverridable = overridableType.contains(elementName);
        Scope scope = Optional.ofNullable(strategy.scope).orElse(GLOBAL_SCOPE);
        if (isOverridable && !SCOPE_GLOBAL_TAG.equals(scope.tag)) {
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
