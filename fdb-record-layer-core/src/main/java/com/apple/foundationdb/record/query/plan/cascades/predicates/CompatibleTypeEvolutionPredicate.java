/*
 * CompatibleTypeEvolutionPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.planprotos.PCompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.planprotos.PCompatibleTypeEvolutionPredicate.PRecordTypeNameFieldAccessPair;
import com.apple.foundationdb.record.planprotos.PFieldAccessTrieNode;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FirstOrDefaultValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.TrieNode;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A predicate to be used as part of a {@link com.apple.foundationdb.record.query.plan.QueryPlanConstraint}
 * which can determine if a given plan can be executed under the types currently defined in the schema.
 * <br>
 * The idea is the following. A record query plan {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}
 * as result of planning a query using a {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} can
 * be introspected by {@link com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty} to
 * produce a trie of field accesses for each record type that is being accessed in the plan. A field access is a path
 * from the top level record of a record type to an accessed field which can be nested many levels deep. For each such
 * path we denote the name of and the ordinal number of each field access for every step along the way. In order to not
 * just have a huge set of such paths, we condense these paths into a trie data structure ({@link FieldAccessTrieNode}.
 * A {@link com.apple.foundationdb.record.query.plan.QueryPlanConstraint} is then created using this predicate that
 * in turn encodes all field accesses of a plan.
 * <br>
 * Once the plan and its constraints are created, it can be executed, and later continued, cached, passed back and forth
 * through different systems and in general be executed again at a later time. Unfortunately, other things may happen
 * concurrently that we need to guard against. The record type definitions may change between planning time and
 * execution time of a query. A plan may be invalidated by some change to the metadata, a dropped index or similar, and
 * must not be executed anymore.
 * <br>
 * One such change that may or may not be benign is an evolving schema. A record type may be amended to include a new
 * field, a field may be dropped, or the type itself is dropped. A newly added field may be benign if it does not shift
 * existing fields accessed by the query, a dropped record type needs to invalidate the plan if the plan needs to access
 * it.
 * <br>
 * The runtime system is expected to test all of a plan's
 * {@link com.apple.foundationdb.record.query.plan.QueryPlanConstraint}s prior to the execution of that plan.
 * This predicate evaluates to {@code true}, if and only if a possible type evolution did not change any of the field
 * accesses in incompatible ways.
 */
@API(API.Status.EXPERIMENTAL)
public class CompatibleTypeEvolutionPredicate extends AbstractQueryPredicate implements LeafQueryPredicate {
    /**
     * The hash value of this predicate.
     */
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Compatible-Type-Evolution-Predicate");

    @Nonnull
    private final Map<String, FieldAccessTrieNode> recordTypeNameFieldAccessMap;

    /**
     * Constructs a new {@link CompatibleTypeEvolutionPredicate} instance.
     * @param recordTypeNameFieldAccessMap a map from record type names to field access tries that for each referenced
     *        record type in a query (designated by its record type name) encodes the accessed fields of that record
     *        type in that query.
     */
    public CompatibleTypeEvolutionPredicate(@Nonnull final Map<String, FieldAccessTrieNode> recordTypeNameFieldAccessMap) {
        super(true);
        this.recordTypeNameFieldAccessMap = ImmutableMap.copyOf(recordTypeNameFieldAccessMap);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context) {
        final RecordMetaData recordMetaData = store.getRecordMetaData();
        final Map<String, RecordType> currentRecordTypes = recordMetaData.getRecordTypes();
        for (final Map.Entry<String, FieldAccessTrieNode> entry : recordTypeNameFieldAccessMap.entrySet()) {
            final FieldAccessTrieNode fieldAccessTrieNode = entry.getValue();
            if (!currentRecordTypes.containsKey(entry.getKey())) {
                // type was dropped
                return false;
            }
            final Type.Record currentType =
                    Type.Record.fromFieldDescriptorsMap(recordMetaData.getFieldDescriptorMapFromNames(ImmutableList.of(entry.getKey())));
            if (!isAccessCompatibleWithCurrentType(fieldAccessTrieNode, currentType)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int computeSemanticHashCode() {
        return LeafQueryPredicate.super.computeSemanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH,
                super.hashCodeWithoutChildren(), recordTypeNameFieldAccessMap);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, isAtomic(), recordTypeNameFieldAccessMap);
    }

    @Override
    public String toString() {
        return "compatibleTypeEvolution()";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final QueryPredicate other,
                                                       @Nonnull final ValueEquivalence valueEquivalence) {
        return super.equalsWithoutChildren(other, valueEquivalence)
                .filter(ignored -> {
                    final CompatibleTypeEvolutionPredicate otherCompatibleTypeEvolutionPredicate = (CompatibleTypeEvolutionPredicate)other;
                    return recordTypeNameFieldAccessMap.equals(
                            otherCompatibleTypeEvolutionPredicate.recordTypeNameFieldAccessMap);
                });
    }

    @Nonnull
    @Override
    public PCompatibleTypeEvolutionPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PCompatibleTypeEvolutionPredicate.Builder builder = PCompatibleTypeEvolutionPredicate.newBuilder();
        for (final Map.Entry<String, FieldAccessTrieNode> entry : recordTypeNameFieldAccessMap.entrySet()) {
            builder.addRecordTypeNameFieldAccessPairs(PRecordTypeNameFieldAccessPair.newBuilder()
                    .setRecordTypeName(entry.getKey())
                    .setFieldAccessTrieNode(entry.getValue().toProto(serializationContext)));
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setCompatibleTypeEvolutionPredicate(toProto(serializationContext)).build();
    }

    @Nonnull
    public static CompatibleTypeEvolutionPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                             @Nonnull final PCompatibleTypeEvolutionPredicate compatibleTypeEvolutionPredicateProto) {
        final ImmutableMap.Builder<String, FieldAccessTrieNode> mapBuilder = ImmutableMap.builder();
        for (int i = 0; i < compatibleTypeEvolutionPredicateProto.getRecordTypeNameFieldAccessPairsCount(); i ++) {
            final PRecordTypeNameFieldAccessPair recordTypeNameFieldAccessPair =
                    compatibleTypeEvolutionPredicateProto.getRecordTypeNameFieldAccessPairs(i);
            mapBuilder.put(Objects.requireNonNull(recordTypeNameFieldAccessPair.getRecordTypeName()),
                    FieldAccessTrieNode.fromProto(serializationContext, Objects.requireNonNull(recordTypeNameFieldAccessPair.getFieldAccessTrieNode())));
        }
        return new CompatibleTypeEvolutionPredicate(mapBuilder.build());
    }

    @Nonnull
    public static Map<String /* RecordTypeName */, FieldAccessTrieNode> computeFieldAccesses(@Nonnull final List<Value> derivationValues) {
        final var buildersMap = Maps.<String /* RecordTypeName */, FieldAccessTrieNodeBuilder>newLinkedHashMap();
        derivationValues.forEach(derivationValue -> computeFieldAccessForDerivation(buildersMap, derivationValue));
        final var resultMapBuilder = ImmutableMap.<String, FieldAccessTrieNode>builder();
        for (final var entry : buildersMap.entrySet()) {
            resultMapBuilder.put(entry.getKey(), entry.getValue().build());
        }
        return resultMapBuilder.build();
    }

    @Nonnull
    private static List<FieldAccessTrieNodeBuilder> computeFieldAccessForDerivation(@Nonnull final Map<String /* RecordTypeName */, FieldAccessTrieNodeBuilder> recordTypeNameTrieBuilderMap,
                                                                                    @Nonnull final Value derivationValue) {
        if (derivationValue instanceof QueriedValue) {
            final var queriedValue = (QueriedValue)derivationValue;
            final var recordTypeNames = queriedValue.getRecordTypeNames();
            if (recordTypeNames != null) {
                final var resultTrieBuilders = ImmutableList.<FieldAccessTrieNodeBuilder>builder();
                for (final String recordTypeName : recordTypeNames) {
                    final var trieBuilder =
                            recordTypeNameTrieBuilderMap.computeIfAbsent(recordTypeName, rTN -> new FieldAccessTrieNodeBuilder(queriedValue.getResultType()));
                    resultTrieBuilders.add(trieBuilder);
                }
                return resultTrieBuilders.build();
            }
            return ImmutableList.of();
        }

        if (derivationValue instanceof LeafValue) {
            return ImmutableList.of();
        }

        final var nestedResultsbuilder = ImmutableList.<List<FieldAccessTrieNodeBuilder>>builder();
        for (final Value child : derivationValue.getChildren()) {
            nestedResultsbuilder.add(computeFieldAccessForDerivation(recordTypeNameTrieBuilderMap, child));
        }
        final var nestedResults = nestedResultsbuilder.build();

        if (derivationValue instanceof FieldValue) {
            Verify.verify(nestedResults.size() == 1);
            final var nestedTrieBuilders = nestedResults.get(0);
            final var fieldValue = (FieldValue)derivationValue;
            final var fieldPath = fieldValue.getFieldPath();
            final var resultTrieBuilders = ImmutableList.<FieldAccessTrieNodeBuilder>builder();
            for (final var nestedTrieBuilder : nestedTrieBuilders) {
                var currentTrieBuilder = nestedTrieBuilder;
                for (final var fieldAccessor : fieldPath.getFieldAccessors()) {
                    var type = currentTrieBuilder.getCurrentType();
                    while (type.isArray()) {
                        type = Objects.requireNonNull(((Type.Array)type).getElementType());
                    }
                    Verify.verify(type.isRecord());
                    final var field = ((Type.Record)type).getField(fieldAccessor.getOrdinal());
                    currentTrieBuilder =
                            currentTrieBuilder.compute(FieldValue.ResolvedAccessor.of(field.getFieldName(), fieldAccessor.getOrdinal(), fieldAccessor.getType()),
                                    (resolvedAccessor, oldTrieBuilder) -> {
                                        if (oldTrieBuilder == null) {
                                            return new FieldAccessTrieNodeBuilder(null, null, field.getFieldType());
                                        }
                                        if (oldTrieBuilder.getValue() != null && !oldTrieBuilder.getValue().isAny()) {
                                            return oldTrieBuilder;
                                        }
                                        oldTrieBuilder.setValue(Type.any());
                                        return oldTrieBuilder;
                                    });
                }
                resultTrieBuilders.add(currentTrieBuilder);
            }
            return resultTrieBuilders.build();
        }

        if (derivationValue instanceof FirstOrDefaultValue) {
            Verify.verify(nestedResults.size() == 2);
            return nestedResults.get(0);
        }

        //
        // For other values we need to decide if they constitute as type-sensitive or not.
        //
        if (derivationValue instanceof RecordConstructorValue) {
            terminateBuilders(nestedResults, builder -> Type.any());
            return ImmutableList.of();
        }

        //
        // default case
        //
        terminateBuilders(nestedResults, FieldAccessTrieNodeBuilder::getCurrentType);
        return ImmutableList.of();
    }

    private static void terminateBuilders(@Nonnull final ImmutableList<List<FieldAccessTrieNodeBuilder>> nestedResults,
                                          final Function<FieldAccessTrieNodeBuilder, Type> typeFunction) {
        for (final var nestedResult : nestedResults) {
            for (final var nestedTrieBuilder : nestedResult) {
                nestedTrieBuilder.setValue(typeFunction.apply(nestedTrieBuilder));
            }
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isAccessCompatibleWithCurrentType(@Nonnull final FieldAccessTrieNode fieldAccessTrieNode,
                                                            @Nonnull Type currentType) {
        if (fieldAccessTrieNode.getChildrenMap() != null) {
            while (currentType.isArray()) {
                currentType = Objects.requireNonNull(((Type.Array)currentType).getElementType());
            }
            Verify.verify(currentType.isRecord());
            final Type.Record currentRecordType = (Type.Record)currentType;
            final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMap = fieldAccessTrieNode.getChildrenMap();
            final Map<String, Type.Record.Field> fieldNameFieldMap = currentRecordType.getFieldNameFieldMap();
            final Map<String, Integer> fieldNameOrdinalMap = currentRecordType.getFieldNameToOrdinalMap();
            for (final Map.Entry<FieldValue.ResolvedAccessor, FieldAccessTrieNode> entry : childrenMap.entrySet()) {
                final FieldValue.ResolvedAccessor resolvedAccessor = entry.getKey();
                final String name = resolvedAccessor.getName();
                final Integer ordinalInCurrentRecordType = fieldNameOrdinalMap.get(name);
                if (ordinalInCurrentRecordType == null) {
                    // field is gone?
                    return false;
                }
                if (ordinalInCurrentRecordType != resolvedAccessor.getOrdinal()) {
                    // field is shifted
                    return false;
                }

                final Type.Record.Field fieldInCurrentRecordType = fieldNameFieldMap.get(name);
                if (!isAccessCompatibleWithCurrentType(entry.getValue(), fieldInCurrentRecordType.getFieldType())) {
                    // something wrong downstream
                    return false;
                }
            }
            return true;
        } else {
            final Type type = Objects.requireNonNull(fieldAccessTrieNode.getValue());
            if (type.isAny()) {
                return true;
            }
            return type.equals(currentType);
        }
    }

    @Nonnull
    public static CompatibleTypeEvolutionPredicate fromPlan(@Nonnull final RecordQueryPlan plannedPlan) {
        final var derivations = DerivationsProperty.evaluateDerivations(plannedPlan);
        final var simplifiedLocalValues = derivations.simplifyLocalValues();
        final var fieldAccesses = CompatibleTypeEvolutionPredicate.computeFieldAccesses(simplifiedLocalValues);
        return new CompatibleTypeEvolutionPredicate(fieldAccesses);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCompatibleTypeEvolutionPredicate, CompatibleTypeEvolutionPredicate> {
        @Nonnull
        @Override
        public Class<PCompatibleTypeEvolutionPredicate> getProtoMessageClass() {
            return PCompatibleTypeEvolutionPredicate.class;
        }

        @Nonnull
        @Override
        public CompatibleTypeEvolutionPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                          @Nonnull final PCompatibleTypeEvolutionPredicate compatibleTypeEvolutionPredicateProto) {
            return CompatibleTypeEvolutionPredicate.fromProto(serializationContext, compatibleTypeEvolutionPredicateProto);
        }
    }

    /**
     * Trie to capture the paths this value needs to check for compatible type evolution.
     */
    public static class FieldAccessTrieNode extends TrieNode.AbstractTrieNode<FieldValue.ResolvedAccessor, Type, FieldAccessTrieNode> implements PlanHashable, PlanSerializable {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Access-Trie-Node");

        private FieldAccessTrieNode(@Nullable final Type type,
                                    @Nullable final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMap) {
            super(type, childrenMap);
        }

        @Nonnull
        @Override
        public FieldAccessTrieNode getThis() {
            return this;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return PlanHashable.objectsPlanHash(mode, BASE_HASH, getValue(), getChildrenMap());
        }

        @Nonnull
        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMap = getChildrenMap();
            if (childrenMap != null) {
                int size = childrenMap.size();
                int i = 0;
                for (final var entry : childrenMap.entrySet()) {
                    builder.append("[").append(entry.getKey().toString()).append("]→[").append(entry.getValue()).append("]");
                    if (i + 1 < size) {
                        builder.append(",");
                    }
                    i ++;
                }
            } else {
                builder.append(getValue());
            }
            return builder.toString();
        }

        @Nonnull
        @Override
        public PFieldAccessTrieNode toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PFieldAccessTrieNode.Builder builder = PFieldAccessTrieNode.newBuilder();
            final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMap = getChildrenMap();
            builder.setChildrenMapIsNull(childrenMap == null);
            if (childrenMap != null) {
                for (final Map.Entry<FieldValue.ResolvedAccessor, FieldAccessTrieNode> entry : childrenMap.entrySet()) {
                    builder.addChildPair(PFieldAccessTrieNode.PResolvedAccessorChildPair.newBuilder()
                            .setResolvedAccessor(entry.getKey().toProto(serializationContext))
                            .setChildFieldAccessTrieNode(entry.getValue().toProto(serializationContext)));
                }
            }
            if (getValue() != null) {
                builder.setType(getValue().toTypeProto(serializationContext));
            }
            return builder.build();
        }

        @Nonnull
        public static FieldAccessTrieNode fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PFieldAccessTrieNode fieldAccessTrieNodeProto) {
            Verify.verify(fieldAccessTrieNodeProto.hasChildrenMapIsNull());
            final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMap;
            if (fieldAccessTrieNodeProto.getChildrenMapIsNull()) {
                childrenMap = null;
            } else {
                final ImmutableMap.Builder<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMapBuilder =
                        ImmutableMap.builder();
                for (int i = 0; i < fieldAccessTrieNodeProto.getChildPairCount(); i ++) {
                    final PFieldAccessTrieNode.PResolvedAccessorChildPair childPair = fieldAccessTrieNodeProto.getChildPair(i);
                    childrenMapBuilder.put(FieldValue.ResolvedAccessor.fromProto(serializationContext, Objects.requireNonNull(childPair.getResolvedAccessor())),
                            FieldAccessTrieNode.fromProto(serializationContext, Objects.requireNonNull(childPair.getChildFieldAccessTrieNode())));
                }
                childrenMap = childrenMapBuilder.build();
            }
            final Type type;
            if (fieldAccessTrieNodeProto.hasType()) {
                type = Type.fromTypeProto(serializationContext, fieldAccessTrieNodeProto.getType());
            } else {
                type = null;
            }
            return new FieldAccessTrieNode(type, childrenMap);
        }

        @Nonnull
        public static FieldAccessTrieNode of(@Nonnull final Type type,
                                             @Nullable final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNode> childrenMap) {
            return new FieldAccessTrieNode(type, childrenMap);
        }
    }

    /**
     * Builder version of {@link FieldAccessTrieNode}.
     */
    public static class FieldAccessTrieNodeBuilder extends TrieNode.AbstractTrieNodeBuilder<FieldValue.ResolvedAccessor, Type, FieldAccessTrieNodeBuilder> {
        @Nonnull
        private final Type currentType;

        public FieldAccessTrieNodeBuilder(@Nonnull final Type currentType) {
            this(null, null, currentType);
        }

        public FieldAccessTrieNodeBuilder(@Nullable final Type type,
                                          @Nullable final Map<FieldValue.ResolvedAccessor, FieldAccessTrieNodeBuilder> childrenMap,
                                          @Nonnull final Type currentType) {
            super(type, childrenMap);
            this.currentType = currentType;
        }

        @Nonnull
        @Override
        public FieldAccessTrieNodeBuilder getThis() {
            return this;
        }

        @Nonnull
        public Type getCurrentType() {
            return currentType;
        }

        @Nonnull
        public FieldAccessTrieNode build() {
            if (getChildrenMap() != null) {
                final var childrenMapBuilder = ImmutableMap.<FieldValue.ResolvedAccessor, FieldAccessTrieNode>builder();
                for (final Map.Entry<FieldValue.ResolvedAccessor, FieldAccessTrieNodeBuilder> entry : getChildrenMap().entrySet()) {
                    childrenMapBuilder.put(entry.getKey(), entry.getValue().build());
                }
                return new FieldAccessTrieNode(null, childrenMapBuilder.build());
            } else {
                // an non-terminated trie is auto-terminated here
                return new FieldAccessTrieNode(getCurrentType(), null);
            }
        }
    }
}
