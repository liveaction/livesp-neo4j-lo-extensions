package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.livingobjects.neo4j.model.export.CrossRelationship;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.KEYTYPE_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._DEFAULT_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._OVERRIDABLE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.PARENT;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class MetaSchema {

    private final InternalNode theGlobalNode;
    private final ImmutableSet<String> overridableType;
    private final ImmutableMap<String, ImmutableSet<String>> requiredProperties;
    private final ImmutableMap<String, Optional<String>> defaultScopes;
    private final ImmutableSet<String> scopeTypes;
    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    // origin_type -> <dest_type, rel_type>
    private final ImmutableMap<String, ImmutableSet<Tuple2<String, String>>> crossAttributesRelations;

    private final ImmutableList<ImmutableList<String>> metaLineages;

    // Comparator for 2 attributes: o1 > o2 => o2 -[:Parent*0..n]-> o1
    public final Comparator<String> lineageComparator = (o1, o2) -> {
        if (recursiveLineageComparator(o1, o2)) {
            return 1;
        } else if (recursiveLineageComparator(o2, o1)) {
            return -1;
        } else {
            return o1.compareTo(o2);
        }
    };

    public MetaSchema(Transaction tx) {
        List<InternalNode> nodes = Lists.newArrayList();
        List<InternalRelation> rels = Lists.newArrayList();
        this.theGlobalNode = new InternalNode(tx.findNode(Labels.SCOPE, TAG, GLOBAL_SCOPE.tag), nodes, rels);
        Objects.requireNonNull(this.theGlobalNode, "Global Scope node not found in database !");

        ImmutableMap.Builder<String, Node> importableKeyTypesBldr = ImmutableMap.builder();
        ImmutableSet.Builder<String> overrideBldr = ImmutableSet.builder();
        ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, ImmutableSet<Tuple2<String, String>>> crossAttributesRelationsBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, ImmutableSet<String>> requiredPropertiesBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, Optional<String>> defaultScopesBldr = ImmutableMap.builder();
        ImmutableMap.Builder<Node, String> scopesBldr = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> scopeByKeyTypesBldr = ImmutableMap.builder();
        ImmutableList.Builder<ImmutableList<String>> metaLineagesBuilder = ImmutableList.builder();

        tx.findNodes(Labels.ATTRIBUTE).forEachRemaining(node -> {
            InternalNode n = new InternalNode(node, nodes, rels);
            String keytype = n.getProperty(_TYPE).toString();
            String key = keytype + KEYTYPE_SEPARATOR + n.getProperty(NAME).toString();

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
                String classAsString = defaultScopeAsString.equals("global") ?
                        "scope" :
                        "cluster";

                defaultScopesBldr.put(key, Optional.of(classAsString + KEYTYPE_SEPARATOR + defaultScopeAsString));
            }

            boolean isOverride = (boolean) n.getProperty(_OVERRIDABLE, false);
            if (isOverride) overrideBldr.add(key);

            if (GraphModelConstants.KEY_TYPES.contains(keytype)) {
                if (GraphModelConstants.IMPORTABLE_KEY_TYPES.contains(keytype)) {
                    importableKeyTypesBldr.put(key, n);
                }
                ImmutableList.Builder<Relationship> crels = ImmutableList.builder();
                ImmutableList.Builder<Relationship> prels = ImmutableList.builder();
                n.getRelationships(INCOMING, PARENT).forEach(crels::add);
                n.getRelationships(OUTGOING, PARENT).forEach(prels::add);
                ImmutableSet<Tuple2<String, String>> crossAttributes = CsvLoaderHelper.getCrossAttributes(n);
                if (!GraphModelConstants.LABEL_TYPE.equals(keytype) && prels.build().isEmpty()) {
                    scopesBldr.put(n, key);
                }
                ImmutableList<Relationship> childrenRelations = crels.build();
                childrenRelationsBldr.put(key, childrenRelations);
                parentRelationsBldr.put(key, prels.build());
                crossAttributesRelationsBldr.put(key, crossAttributes);

                if (childrenRelations.isEmpty()) {
                    metaLineagesBuilder.addAll(generateMetaLineages(n, ImmutableList.of()));
                }
            }
        });

        this.overridableType = overrideBldr.build();
        this.childrenRelations = childrenRelationsBldr.build();
        this.parentRelations = parentRelationsBldr.build();
        this.crossAttributesRelations = crossAttributesRelationsBldr.build();
        this.metaLineages = metaLineagesBuilder.build();

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

    /**
     * Generates all lineages for the given node
     */
    private ImmutableList<ImmutableList<String>> generateMetaLineages(Node current, ImmutableList<String> previous) {
        String key = current.getProperty(_TYPE).toString() + KEYTYPE_SEPARATOR + current.getProperty(NAME).toString();
        ImmutableList.Builder<ImmutableList<String>> builder = ImmutableList.builder();
//        ImmutableList<String> currentLineage = ImmutableList.<String>builder()
//                .addAll(previous)
//                .add(key)
//                .build();
        List<Relationship> relationships = Lists.newArrayList(current.getRelationships(OUTGOING, PARENT));

        if (relationships.isEmpty()) {
            builder.add(ImmutableList.of(key));
        } else {
            relationships.forEach(r -> builder.addAll(generateMetaLineages(r.getEndNode(), null)
                    .stream()
                    .map(list -> ImmutableList.<String>builder().add(key).addAll(list).build())
                    .collect(Collectors.toList())));
        }
        return builder.build();
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

    public ImmutableMap<String, ImmutableSet<Tuple2<String, String>>> getCrossAttributesRelations() {
        return crossAttributesRelations;
    }

    public ImmutableSet<String> getCrossAttributesRelations(String keyAttribute) {
        Set<String> collect = crossAttributesRelations.entrySet().stream()
                .filter(entry -> entry.getKey().equals(keyAttribute))
                .flatMap(entry -> entry.getValue().stream())
                .map(Tuple2::_1)
                .collect(Collectors.toSet());
        return ImmutableSet.copyOf(collect);
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
        return node.getProperty(_TYPE).toString() + KEYTYPE_SEPARATOR + node.getProperty(NAME).toString();
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

    public ImmutableList<ImmutableList<String>> getMetaLineagesForType(String type) {
        List<ImmutableList<String>> result = metaLineages.stream()
                .filter(lineage -> lineage.contains(type))
                .collect(Collectors.toList());
        return ImmutableList.copyOf(result);
    }

    /**
     * Returns all the relationships existing with given type
     */
    public CrossRelationship getRelationshipOfType(String reltype) {
        return crossAttributesRelations.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .filter(e -> e.getKey().equals(reltype) || e.getValue().stream()
                        .anyMatch(tuple -> tuple._2().equals(reltype)))
                .flatMap(e -> e.getValue().stream()
                        .filter(tuple -> tuple._2().equals(reltype))
                        .map(tuple -> new CrossRelationship(e.getKey(), tuple._1, tuple._2)))
                .collect(MoreCollectors.onlyElement());
    }

    /**
     * returns a List of all the path possible between the originType and destType. Each path contains a List of all types (in the order from bottom to top) needed to get to destination.
     * If destType is not a parent of originType, this method will always return an empty List
     * If destType and originType are the same, this method will return a List containing the empty List
     */
    public ImmutableList<ImmutableList<String>> getUpwardPath(String originType, String destType) {
        if (destType.equals(originType)) {
            return ImmutableList.of(ImmutableList.of());
        }
        return ImmutableList.copyOf(getParentRelations()
                .get(originType)
                .stream()
                .map(Relationship::getEndNode)
                .map(this::getKeyAttribute)
                .flatMap(parentType -> parentType.equals(destType) ?
                        ImmutableList.of(ImmutableList.of(parentType)).stream() :
                        getUpwardPath(parentType, destType).stream()
                                .map(path -> ImmutableList.<String>builder()
                                        .add(parentType)
                                        .addAll(path)
                                        .build()))
                .collect(Collectors.toList()));
    }

    private boolean recursiveLineageComparator(String o1, String o2) {
        if (getMonoParentRelations(o1)
                .anyMatch(o2::equals)) {
            return true;
        } else {
            return getMonoParentRelations(o1)
                    .anyMatch(s -> recursiveLineageComparator(s, o2));
        }
    }

    public class InternalRelation implements Relationship {
        private Map<String, Object> properties;
        private InternalNode startNode;
        private InternalNode endNode;
        private RelationshipType type;
        private long id;

        private InternalRelation(Relationship r, List<InternalNode> nodes, List<InternalRelation> rels) {
            rels.add(this);
            id = r.getId();
            type = r.getType();
            properties = r.getAllProperties();
            // get start node from already created ones
            startNode = nodes.stream()
                    .filter(n -> n.getId() == r.getStartNode().getId())
                    .findFirst()
                    .or(() -> Optional.of(new InternalNode(r.getStartNode(), nodes, rels)))
                    .get();
            // get end node from already created ones
            endNode = nodes.stream()
                    .filter(n -> n.getId() == r.getEndNode().getId())
                    .findFirst()
                    .or(() -> Optional.of(new InternalNode(r.getEndNode(), nodes, rels)))
                    .get();
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Node getStartNode() {
            return startNode;
        }

        @Override
        public Node getEndNode() {
            return endNode;
        }

        @Override
        public Node getOtherNode(Node node) {
            if (node.equals(startNode)) {
                return endNode;
            }
            if (node.equals(endNode)) {
                return startNode;
            }
            throw new IllegalArgumentException("Node should be either startNode or endNode");
        }

        @Override
        public Node[] getNodes() {
            return new Node[]{startNode, endNode};
        }

        @Override
        public RelationshipType getType() {
            return type;
        }

        @Override
        public boolean isType(RelationshipType type) {
            return this.type.equals(type);
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public boolean hasProperty(String key) {
            return properties.containsKey(key);
        }

        @Override
        public Object getProperty(String key) {
            return properties.get(key);
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return properties.getOrDefault(key, defaultValue);
        }

        @Override
        public void setProperty(String key, Object value) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Object removeProperty(String key) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return properties.keySet();
        }

        @Override
        public Map<String, Object> getProperties(String... keys) {
            List<String> keyList = List.of(keys);
            return properties.entrySet()
                    .stream()
                    .filter(entry -> keyList.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return properties;
        }
    }

    public class InternalNode implements Node {
        private List<Relationship> incRelations;
        private List<Relationship> outRelations;
        private List<Relationship> allRelations;
        private Iterable<Label> labels;
        private long id;
        private Map<String, Object> properties;

        protected InternalNode(Node node, List<InternalNode> nodes, List<InternalRelation> rels) {
            nodes.add(this);
            id = node.getId();
            labels = node.getLabels();
            properties = node.getAllProperties();
            Function<Relationship, InternalRelation> relationToInternalRelation = r -> rels.stream()
                    .filter(rel -> r.getId() == rel.getId())
                    .findFirst()
                    .or(() -> Optional.of(new InternalRelation(r, nodes, rels)))
                    .get();
            incRelations = Streams.stream(node.getRelationships(INCOMING))
                    .map(relationToInternalRelation)
                    .collect(Collectors.toList());
            outRelations = Streams.stream(node.getRelationships(OUTGOING)).map(relationToInternalRelation)
                    .collect(Collectors.toList());
            allRelations = Streams.stream(node.getRelationships()).map(relationToInternalRelation)
                    .collect(Collectors.toList());
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Iterable<Relationship> getRelationships() {
            return allRelations;
        }

        @Override
        public boolean hasRelationship() {
            return allRelations.isEmpty();
        }

        @Override
        public Iterable<Relationship> getRelationships(RelationshipType... types) {
            return getRelation(allRelations, types);
        }

        @Override
        public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
            switch (direction) {
                case BOTH:
                    return getRelation(allRelations, types);
                case INCOMING:
                    return getRelation(incRelations, types);
                case OUTGOING:
                    return getRelation(outRelations, types);
            }
            return null;
        }

        @Override
        public boolean hasRelationship(RelationshipType... types) {
            return containsRelation(allRelations, types);
        }

        @Override
        public boolean hasRelationship(Direction direction, RelationshipType... types) {
            switch (direction) {
                case BOTH:
                    return containsRelation(allRelations, types);
                case INCOMING:
                    return containsRelation(incRelations, types);
                case OUTGOING:
                    return containsRelation(outRelations, types);
            }
            return false;
        }

        private boolean containsRelation(List<Relationship> relations, RelationshipType... types) {
            List<RelationshipType> typeList = List.of(types);
            return relations.stream()
                    .anyMatch(r -> typeList.contains(r.getType()));
        }

        private List<Relationship> getRelation(List<Relationship> relations, RelationshipType... types) {
            List<RelationshipType> typeList = List.of(types);
            return relations.stream()
                    .filter(r -> typeList.contains(r.getType()))
                    .collect(Collectors.toList());
        }

        @Override
        public Iterable<Relationship> getRelationships(Direction dir) {
            switch (dir) {
                case BOTH:
                    return allRelations;
                case INCOMING:
                    return incRelations;
                case OUTGOING:
                    return outRelations;
            }
            return null;
        }

        @Override
        public boolean hasRelationship(Direction dir) {
            switch (dir) {
                case BOTH:
                    return !allRelations.isEmpty();
                case INCOMING:
                    return !incRelations.isEmpty();
                case OUTGOING:
                    return !outRelations.isEmpty();
            }
            return false;
        }

        @Override
        public Relationship getSingleRelationship(RelationshipType type, Direction dir) {

            ArrayList<Relationship> relationships = Lists.newArrayList(getRelationships(dir, type));
            if (relationships.size() != 1) {
                throw new IllegalArgumentException(String.format("Only 1 relationship should match type %s and direction %s but %d were found", type, dir, relationships.size()));
            }
            return relationships.get(0);
        }

        @Override
        public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes() {
            return allRelations.stream().map(Relationship::getType).collect(Collectors.toSet());
        }

        @Override
        public int getDegree() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public int getDegree(RelationshipType type) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public int getDegree(Direction direction) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public int getDegree(RelationshipType type, Direction direction) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void addLabel(Label label) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public void removeLabel(Label label) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public boolean hasLabel(Label label) {
            for (Label l : getLabels()) {
                if (l.equals(label)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Iterable<Label> getLabels() {
            return labels;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public boolean hasProperty(String key) {
            for (String p : getPropertyKeys()) {
                if (p.equals(key)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Object getProperty(String key) {
            return properties.get(key);
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return properties.getOrDefault(key, defaultValue);
        }

        @Override
        public void setProperty(String key, Object value) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Object removeProperty(String key) {
            throw new UnsupportedOperationException("Can't perform update operations on Internal entities");
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return properties.keySet();
        }

        @Override
        public Map<String, Object> getProperties(String... keys) {
            List<String> keyList = List.of(keys);
            return properties.entrySet()
                    .stream()
                    .filter(entry -> keyList.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return properties;
        }
    }
}
