package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._OVERRIDABLE;
import static org.neo4j.graphdb.Direction.INCOMING;

public final class MetaSchema {

    private final Node theGlobalNode;
    private final ImmutableSet<String> overridableType;

    public final ImmutableSet<String> scopeTypes;

    public final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    public final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    public final ImmutableMap<String, ImmutableSet<String>> crossAttributesRelations;
    public final ImmutableMap<String, String> scopeByKeyTypes;

    MetaSchema(GraphDatabaseService graphDb) {
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

        ImmutableMap<Node, String> scopes = scopesBldr.build();
        for (Map.Entry<String, Node> attributeNodeEntry : importableKeyTypesBldr.build().entrySet()) {
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

    public Node getTheGlobalScopeNode() {
        return theGlobalNode;
    }

    public boolean isOverridable(String elementKeyType) {
        return overridableType.contains(elementKeyType);
    }

    public boolean isScope(String elementKeyType) {
        return scopeTypes.contains(elementKeyType);
    }

    public ImmutableSet<String> getCrossAttributesRelations(String keyAttribute) {
        return crossAttributesRelations.get(keyAttribute);
    }

    private Optional<String> getScopeContext(ImmutableMap<Node, String> scopes, Node attributeNode) {
        return IWanLoaderHelper.getParent(attributeNode)
                .map(node -> getScopeContext(scopes, node))
                .orElseGet(() -> Optional.ofNullable(scopes.get(attributeNode)));
    }

    public Stream<String> getMonoParentRelations(String keyAttribute) {
        return filterParentRelations(keyAttribute, cardinality -> !IwanModelConstants.CARDINALITY_MULTIPLE.equals(cardinality));
    }

    public Optional<String> getRequiredParent(String keyAttribute) {
        return filterParentRelations(keyAttribute, cardinality -> cardinality == null || IwanModelConstants.CARDINALITY_UNIQUE_PARENT.equals(cardinality))
                .findFirst();
    }

    private Stream<String> filterParentRelations(String keyAttribute, Function<String, Boolean> cardinalityFilter) {
        return parentRelations.get(keyAttribute).stream()
                .filter(r -> cardinalityFilter.apply(r.getProperty(IwanModelConstants.CARDINALITY, "").toString()))
                .map(this::getKeyAttribute);
    }

    private String getKeyAttribute(Relationship r) {
        return r.getEndNode().getProperty(IwanModelConstants._TYPE).toString() + IwanModelConstants.KEYTYPE_SEPARATOR + r.getEndNode().getProperty(NAME).toString();
    }

}
