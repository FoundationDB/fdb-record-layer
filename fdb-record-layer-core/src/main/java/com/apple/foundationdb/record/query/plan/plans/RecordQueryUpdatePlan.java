/*
 * RecordQueryUpdatePlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryUpdatePlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers.CoercionTrieNode;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers.TransformationTrieNode;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that updates records in the database using a stream of incoming records, that are transformed according
 * to a transformation map, then coerced into a target type, and then saved using
 * {@link FDBRecordStoreBase#saveRecord(Message)}.
 */
@API(API.Status.INTERNAL)
public class RecordQueryUpdatePlan extends RecordQueryAbstractDataModificationPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Update-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUpdatePlan.class);

    protected RecordQueryUpdatePlan(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PRecordQueryUpdatePlan recordQueryUpdatePlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryUpdatePlanProto.getSuper()));
    }

    private RecordQueryUpdatePlan(@Nonnull final Quantifier.Physical inner,
                                  @Nonnull final String targetRecordType,
                                  @Nonnull final Type.Record targetType,
                                  @Nullable final TransformationTrieNode transformationsTrie,
                                  @Nullable final CoercionTrieNode coercionsTrie,
                                  @Nonnull final Value computationValue) {
        super(inner, targetRecordType, targetType, transformationsTrie, coercionsTrie, computationValue, currentModifiedRecordAlias());
    }

    @Override
    public PipelineOperation getPipelineOperation() {
        return PipelineOperation.UPDATE;
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final M message, final boolean isDryRun) {
        if (isDryRun) {
            return store.dryRunSaveRecordAsync(message, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
        } else {
            return store.saveRecordAsync(message, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
        }
    }

    @Nonnull
    @Override
    public RecordQueryUpdatePlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUpdatePlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getTargetRecordType(),
                getTargetType(),
                translateTransformationsTrie(translationMap),
                getCoercionTrie(),
                getComputationValue().translateCorrelations(translationMap));
    }

    @Nullable
    private MessageHelpers.TransformationTrieNode translateTransformationsTrie(final @Nonnull TranslationMap translationMap) {
        final var transformationsTrie = getTransformationsTrie();
        if (transformationsTrie == null) {
            return null;
        }

        return transformationsTrie.<MessageHelpers.TransformationTrieNode>mapMaybe((current, childrenTries) -> {
            final var value = current.getValue();
            if (value != null) {
                Verify.verify(Iterables.isEmpty(childrenTries));
                return new MessageHelpers.TransformationTrieNode(value.translateCorrelations(translationMap), null);
            } else {
                final var oldChildrenMap = Verify.verifyNotNull(current.getChildrenMap());
                final var childrenTriesIterator = childrenTries.iterator();
                final var resultBuilder = ImmutableMap.<Integer, MessageHelpers.TransformationTrieNode>builder();
                for (final var oldEntry : oldChildrenMap.entrySet()) {
                    Verify.verify(childrenTriesIterator.hasNext());
                    final var childTrie = childrenTriesIterator.next();
                    resultBuilder.put(oldEntry.getKey(), childTrie);
                }
                return new MessageHelpers.TransformationTrieNode(null, resultBuilder.build());
            }
        }).orElseThrow(() -> new RecordCoreException("unable to translate correlations"));
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryUpdatePlan(Quantifier.physical(childRef),
                getTargetRecordType(),
                getTargetType(),
                getTransformationsTrie(),
                getCoercionTrie(),
                getComputationValue());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, super.planHash(mode));
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    /**
     * Rewrite the planner graph for better visualization.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models the filter as a node that uses the expression attribute
     *         to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.size() == 1);
        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                                getResultType(),
                                ImmutableList.of(getTargetRecordType())),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("UPDATE"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    @Override
    public PRecordQueryUpdatePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryUpdatePlan.newBuilder().setSuper(toRecordQueryAbstractModificationPlanProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setUpdatePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryUpdatePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PRecordQueryUpdatePlan recordQueryUpdatePlanProto) {
        return new RecordQueryUpdatePlan(serializationContext, recordQueryUpdatePlanProto);
    }

    /**
     * Factory method to create a {@link RecordQueryUpdatePlan}.
     * <br>
     * Example:
     * <pre>
     * {@code
     *    RecordQueryAbstractDataModificationPlan.forTransformMap(inner,
     *                                          ...,
     *                                          ImmutableMap.of(path1, new LiteralValue<>("1"),
     *                                                          path2, new LiteralValue<>(2),
     *                                                          path3, new LiteralValue<>(3)));
     * }
     * </pre>
     * transforms the current inner's object, according to the transform map, i.e. the data underneath {@code path1}
     * is transformed to the value {@code "1"}, the data underneath {@code path2} is transformed to the value {@code 2},
     * and the data underneath {@code path2} is transformed to the value {@code 3}.
     * @param inner an input value to transform
     * @param targetRecordType the name of the record type this update modifies
     * @param targetType a target type to coerce the current record to prior to the update
     * @param transformMap a map of field paths to values.
     * @param computationValue a value to be computed based on the {@code inner} and
     *        {@link RecordQueryAbstractDataModificationPlan#currentModifiedRecordAlias()}
     * @return a newly created {@link RecordQueryUpdatePlan}
     */
    @Nonnull
    public static RecordQueryUpdatePlan updatePlan(@Nonnull final Quantifier.Physical inner,
                                                   @Nonnull final String targetRecordType,
                                                   @Nonnull final Type.Record targetType,
                                                   @Nonnull final Map<FieldValue.FieldPath, Value> transformMap,
                                                   @Nonnull final Value computationValue) {
        final var transformationsTrie = computeTrieForFieldPaths(checkAndPrepareOrderedFieldPaths(transformMap), transformMap);
        return new RecordQueryUpdatePlan(inner,
                targetRecordType,
                targetType,
                transformationsTrie,
                PromoteValue.computePromotionsTrie(targetType, inner.getFlowedObjectType(), transformationsTrie),
                computationValue);
    }

    @Nonnull
    public static List<FieldValue.FieldPath> checkAndPrepareOrderedFieldPaths(@Nonnull final Map<FieldValue.FieldPath, ? extends Value> transformMap) {
        // this brings together all paths that share the same prefixes
        final var orderedFieldPaths =
                transformMap.keySet()
                        .stream()
                        .sorted(FieldValue.FieldPath.comparator())
                        .collect(ImmutableList.toImmutableList());

        FieldValue.FieldPath currentFieldPath = null;
        for (final var fieldPath : orderedFieldPaths) {
            SemanticException.check(currentFieldPath == null || !currentFieldPath.isPrefixOf(fieldPath), SemanticException.ErrorCode.UPDATE_TRANSFORM_AMBIGUOUS);
            currentFieldPath = fieldPath;
        }
        return orderedFieldPaths;
    }

    /**
     * Method to compute a trie from a collection of lexicographically-ordered field paths. The trie is computed at
     * instantiation time (planning time). It serves to transform the input value in one pass.
     *
     * @param orderedFieldPaths a collection of field paths that must be lexicographically-ordered.
     * @param transformMap a map of transformations
     *
     * @return a {@link TransformationTrieNode}
     */
    @Nonnull
    public static TransformationTrieNode computeTrieForFieldPaths(@Nonnull final Collection<FieldValue.FieldPath> orderedFieldPaths,
                                                                  @Nonnull final Map<FieldValue.FieldPath, ? extends Value> transformMap) {
        return computeTrieForFieldPaths(new FieldValue.FieldPath(ImmutableList.of()), transformMap, Iterators.peekingIterator(orderedFieldPaths.iterator()));
    }

    @Nonnull
    private static TransformationTrieNode computeTrieForFieldPaths(@Nonnull final FieldValue.FieldPath prefix,
                                                                   @Nonnull final Map<FieldValue.FieldPath, ? extends Value> transformMap,
                                                                   @Nonnull final PeekingIterator<FieldValue.FieldPath> orderedFieldPathIterator) {
        if (transformMap.containsKey(prefix)) {
            orderedFieldPathIterator.next();
            return new TransformationTrieNode(Verify.verifyNotNull(transformMap.get(prefix)), null);
        }
        final var childrenMapBuilder = ImmutableMap.<Integer, TransformationTrieNode>builder();
        while (orderedFieldPathIterator.hasNext()) {
            final var fieldPath = orderedFieldPathIterator.peek();
            if (!prefix.isPrefixOf(fieldPath)) {
                break;
            }

            final var prefixAccessors = prefix.getFieldAccessors();
            final var currentAccessor = fieldPath.getFieldAccessors().get(prefixAccessors.size());
            final var nestedPrefix = new FieldValue.FieldPath(ImmutableList.<FieldValue.ResolvedAccessor>builder()
                    .addAll(prefixAccessors)
                    .add(currentAccessor)
                    .build());

            final var currentTrie = computeTrieForFieldPaths(nestedPrefix, transformMap, orderedFieldPathIterator);
            childrenMapBuilder.put(currentAccessor.getOrdinal(), currentTrie);
        }

        return new TransformationTrieNode(null, childrenMapBuilder.build());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryUpdatePlan, RecordQueryUpdatePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryUpdatePlan> getProtoMessageClass() {
            return PRecordQueryUpdatePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryUpdatePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PRecordQueryUpdatePlan recordQueryUpdatePlanProto) {
            return RecordQueryUpdatePlan.fromProto(serializationContext, recordQueryUpdatePlanProto);
        }
    }
}
