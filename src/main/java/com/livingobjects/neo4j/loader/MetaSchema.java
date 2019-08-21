package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.CARDINALITY;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.CARDINALITY_UNIQUE_PARENT;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._DEFAULT_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._OVERRIDABLE;
import static org.neo4j.graphdb.Direction.INCOMING;

public final class MetaSchema {

    private final Node theGlobalNode;
    private final ImmutableSet<String> overridableType;
    private final ImmutableMap<String, ImmutableSet<String>> requiredProperties;
    private final ImmutableMap<String, Optional<String>> defaultScopes;

    private final ImmutableSet<String> scopeTypes;

    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    private final ImmutableMap<String, ImmutableSet<String>> crossAttributesRelations;

    public MetaSchema(GraphDatabaseService graphDb) {
        this.theGlobalNode = graphDb.findNode(Labels.SCOPE, TAG, GLOBAL_SCOPE.tag);
        Objects.requireNonNull(this.theGlobalNode, "Global Scope node not found in database !");

        ImmutableMap.Builder<String, Node> importableKeyTypesBldr = ImmutableMap.builder();
        ImmutableSet.Builder<String> overrideBldr = ImmutableSet.builder();
        ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, ImmutableSet<String>> crossAttributesRelationsBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, ImmutableSet<String>> requiredPropertiesBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, Optional<String>> defaultScopesBldr = ImmutableMap.builder();
        ImmutableMap.Builder<Node, String> scopesBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> scopeByKeyTypesBldr = ImmutableMap.builder();
        graphDb.findNodes(Labels.ATTRIBUTE).forEachRemaining(n -> {
            String keytype = n.getProperty(GraphModelConstants._TYPE).toString();
            String key = keytype + GraphModelConstants.KEYTYPE_SEPARATOR + n.getProperty(NAME).toString();

            Object requiredProperties = n.getProperty("requiredProperties", new String[0]);
            if (requiredProperties instanceof String[]) {
                requiredPropertiesBldr.put(key, ImmutableSet.copyOf((String[]) requiredProperties));
            } else {
                requiredPropertiesBldr.put(key, ImmutableSet.of(requiredProperties.toString()));
            }

            Object defaultScope = n.getProperty(_DEFAULT_SCOPE, null);

            if (defaultScope == null) {
                defaultScopesBldr.put(key, Optional.empty());
            } else {
                String defaultScopeAsString = (String) defaultScope;
                defaultScopesBldr.put(key, Optional.of(defaultScopeAsString));
            }

            boolean isOverride = (boolean) n.getProperty(_OVERRIDABLE, false);
            if (isOverride) overrideBldr.add(key);

            if (GraphModelConstants.KEY_TYPES.contains(keytype)) {
                if (GraphModelConstants.IMPORTABLE_KEY_TYPES.contains(keytype)) {
                    importableKeyTypesBldr.put(key, n);
                }
                ImmutableList.Builder<Relationship> crels = ImmutableList.builder();
                ImmutableList.Builder<Relationship> prels = ImmutableList.builder();
                n.getRelationships(INCOMING, RelationshipTypes.PARENT).forEach(crels::add);
                n.getRelationships(Direction.OUTGOING, RelationshipTypes.PARENT).forEach(prels::add);
                ImmutableSet<String> crossAttributes = CsvLoaderHelper.getCrossAttributes(n);
                if (!GraphModelConstants.LABEL_TYPE.equals(keytype) && prels.build().isEmpty()) {
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

        ImmutableMap<Node, String> scopes = scopesBldr.build();
        for (Map.Entry<String, Node> attributeNodeEntry : importableKeyTypesBldr.build().entrySet()) {
            String keyType = attributeNodeEntry.getKey();
            Node attributeNode = attributeNodeEntry.getValue();
            getScopeContext(scopes, attributeNode)
                    .ifPresent(scope -> scopeByKeyTypesBldr.put(keyType, scope));
        }

        Set<String> scopeTypes = Sets.newHashSet();
        scopeTypes.addAll(scopes.values());
        scopeTypes.add(SP_SCOPE.attribute);
        scopeTypes.add(GLOBAL_SCOPE.attribute);
        this.scopeTypes = ImmutableSet.copyOf(scopeTypes);

        this.requiredProperties = requiredPropertiesBldr.build();
        this.defaultScopes = defaultScopesBldr.build();
    }

    public ImmutableSet<String> getScopeTypes() {
        return scopeTypes;
    }

    public Node getTheGlobalScopeNode() {
        return theGlobalNode;
    }

    public boolean isOverridable(String elementKeyType) {
        return overridableType.contains(elementKeyType);
    }

    public boolean isScope(String elementKeyType) {
        return scopeTypes.contains(elementKeyType);
    }

    public final ImmutableSet<String> getRequiredProperties(String keyAttribute) {
        return requiredProperties.getOrDefault(keyAttribute, ImmutableSet.of());
    }

    public ImmutableMap<String, ImmutableSet<String>> getCrossAttributesRelations() {
        return crossAttributesRelations;
    }

    public ImmutableSet<String> getCrossAttributesRelations(String keyAttribute) {
        return crossAttributesRelations.getOrDefault(keyAttribute, ImmutableSet.of());
    }

    public ImmutableList<Relationship> getChildren(String current) {
        return childrenRelations.getOrDefault(current, ImmutableList.of());
    }

    public ImmutableList<String> getAllChildren(String current) {
        return getChildrenOfCardinality(current, (s) -> true);
    }

    public ImmutableList<String> getStrongChildren(String current) {
        return getChildrenOfCardinality(current, CARDINALITY_UNIQUE_PARENT::equals);
    }

    private ImmutableList<String> getChildrenOfCardinality(String current, Predicate<String> expectedCardinaly) {
        return ImmutableList.copyOf(getChildren(current).stream()
                .filter(r -> expectedCardinaly.test(r.getProperty(CARDINALITY, "").toString()))
                .map(Relationship::getStartNode)
                .map(this::getKeyAttribute)
                .flatMap(child -> Stream.concat(Stream.of(child), getStrongChildren(child).stream()))
                .collect(Collectors.toList()));
    }

    public int scopeLevel(String scopeTag) {
        if (scopeTag.equals(GLOBAL_SCOPE.tag)) {
            return 2;
        } else if (scopeTag.equals(SP_SCOPE.tag)) {
            return 1;
        } else {
            return 0;
        }
    }

    private Optional<String> getScopeContext(ImmutableMap<Node, String> scopes, Node attributeNode) {
        return CsvLoaderHelper.getParent(attributeNode)
                .map(node -> getScopeContext(scopes, node))
                .orElseGet(() -> Optional.ofNullable(scopes.get(attributeNode)));
    }

    public Stream<String> getMonoParentRelations(String keyAttribute) {
        return filterParentRelations(keyAttribute, cardinality -> !GraphModelConstants.CARDINALITY_MULTIPLE.equals(cardinality));
    }

    public Optional<String> getRequiredParent(String keyAttribute) {
        return getRequiredParents(keyAttribute)
                .findFirst();
    }

    public ImmutableMap<String, ImmutableList<Relationship>> getParentRelations() {
        return parentRelations;
    }

    public Stream<String> getRequiredParents(String keyAttribute) {
        return filterParentRelations(keyAttribute, cardinality -> cardinality == null || GraphModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality));
    }

    private Stream<String> filterParentRelations(String keyAttribute, Function<String, Boolean> cardinalityFilter) {
        return parentRelations.get(keyAttribute).stream()
                .filter(r -> cardinalityFilter.apply(r.getProperty(GraphModelConstants.CARDINALITY, "").toString()))
                .map(Relationship::getEndNode)
                .map(this::getKeyAttribute);
    }

    private String getKeyAttribute(Node node) {
        return node.getProperty(GraphModelConstants._TYPE).toString() + GraphModelConstants.KEYTYPE_SEPARATOR + node.getProperty(NAME).toString();
    }

    public ImmutableSet<String> getAuthorizedScopes(String keyAttribute) {
        if (scopeTypes.contains(keyAttribute)) {
            return ImmutableSet.of(keyAttribute);
        } else {
            return ImmutableSet.copyOf(getMonoParentRelations(keyAttribute)
                    .flatMap(parent -> {
                        if (scopeTypes.contains(parent)) {
                            return Stream.of(parent);
                        } else {
                            return getAuthorizedScopes(parent).stream();
                        }
                    })
                    .collect(Collectors.toSet()));
        }
    }

    public boolean keyAttributeExists(String keyAttribute) {
        return parentRelations.get(keyAttribute) != null;
    }

    public boolean isMultiScope(String keyAttribute) {
        return getAuthorizedScopes(keyAttribute).size() > 1;
    }

    public Optional<String> getDefaultScope(String keyAttribute) {
        return defaultScopes.get(keyAttribute);
    }
}
