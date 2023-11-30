/*
 * SyntheticRecordPlanner.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A planner for {@link SyntheticRecordPlan} and {@link SyntheticRecordFromStoredRecordPlan} plans.
 *
 * The planner is invoked when plans are needed for index maintenance, either to generate all synthetic
 * records for rebuilding a whole index or to generate affected synthetic records when a stored record
 * is modified (the {@code FromStoredRecord} case). The planner is initialized with a {@link RecordMetaData},
 * for getting synthetic record type definitions (such as the joins that make up a joined record type) and
 * a {@link RecordQueryPlanner}, which is used to plan the underlying scans of stored records from which the
 * records to be indexed can be synthesized.
 */
@API(API.Status.INTERNAL)
public class SyntheticRecordPlanner {

    @Nonnull
    private final RecordMetaData recordMetaData;
    @Nonnull
    private final RecordQueryPlanner queryPlanner;
    @Nullable
    private final FDBStoreTimer timer;

    /**
     * Initialize a new planner.
     * @param recordMetaData meta-data to use for planning
     * @param storeState index enabling state to use for planning
     * @param timer store timer for collecting metrics during planning
     */
    public SyntheticRecordPlanner(@Nonnull RecordMetaData recordMetaData, @Nonnull RecordStoreState storeState, @Nullable FDBStoreTimer timer) {
        this.recordMetaData = recordMetaData;
        this.queryPlanner = new RecordQueryPlanner(recordMetaData, storeState);
        this.timer = timer;
    }

    /**
     * Initialize a new planner.
     * @param store a record store
     */
    public SyntheticRecordPlanner(@Nonnull FDBRecordStore store) {
        this(store.getRecordMetaData(), store.getRecordStoreState(), store.getTimer());
    }

    /**
     * Construct a plan for generating all synthetic records of a given type.
     *
     * The generated records will derive from some stored record in a record store.
     * @param syntheticRecordType the synthetic record type
     * @return a plan that can be applied to a record store to generate synthetic records
     */
    @Nonnull
    public SyntheticRecordPlan scanForType(@Nonnull SyntheticRecordType<?> syntheticRecordType) {
        final SyntheticRecordFromStoredRecordPlan fromRecord = forType(syntheticRecordType);
        // Query to get all records of the needed type(s).
        RecordQueryPlan query = queryPlanner.plan(RecordQuery.newBuilder().setRecordTypes(fromRecord.getStoredRecordTypes()).build());
        return new SyntheticRecordScanPlan(query, fromRecord, true); // TODO: better needDistinct calculation
    }

    /**
     * Construct a plan for generating all synthetic records of a given type.
     *
     * The generated records will derive from some stored record in a record store.
     * @param syntheticRecordType the synthetic record type
     * @return a plan that can be applied to a record store to generate synthetic records
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public SyntheticRecordFromStoredRecordPlan forType(@Nonnull SyntheticRecordType<?> syntheticRecordType) {
        if (syntheticRecordType.getRecordMetaData() != recordMetaData) {
            throw mismatchedMetaData();
        }
        if (syntheticRecordType instanceof JoinedRecordType) {
            return forType((JoinedRecordType)syntheticRecordType);
        }
        if (syntheticRecordType instanceof UnnestedRecordType) {
            return forType((UnnestedRecordType)syntheticRecordType);
        }
        throw unknownSyntheticType(syntheticRecordType);
    }

    /**
     * Construct a plan for generating all joined records of a given type.
     *
     * The generated records will derive from stored records in a record store.
     * @param joinedRecordType the joined record type
     * @return a plan that can be applied to a record store to generate joined records
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public SyntheticRecordFromStoredRecordPlan forType(@Nonnull JoinedRecordType joinedRecordType) {
        if (joinedRecordType.getRecordMetaData() != recordMetaData) {
            throw mismatchedMetaData();
        }
        // If there is some constituent that isn't outer-joined, then we can start with it.
        Optional<JoinedRecordType.JoinConstituent> maybeConstituent = joinedRecordType.getConstituents().stream().filter(c -> !c.isOuterJoined()).findFirst();
        if (maybeConstituent.isPresent()) {
            return forJoinConstituent(joinedRecordType, maybeConstituent.get());
        } else {
            Multimap<String, SyntheticRecordFromStoredRecordPlan> byType = ArrayListMultimap.create();
            for (JoinedRecordType.JoinConstituent joinConstituent : joinedRecordType.getConstituents()) {
                addToByType(byType, joinedRecordType, joinConstituent);
            }
            return createByType(byType);
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public SyntheticRecordFromStoredRecordPlan forType(@Nonnull UnnestedRecordType unnestedRecordType) {
        if (unnestedRecordType.getRecordMetaData() != recordMetaData) {
            throw mismatchedMetaData();
        }
        UnnestedRecordType.NestedConstituent parentConstituent = unnestedRecordType.getParentConstituent();
        return new UnnestStoredRecordPlan(unnestedRecordType, parentConstituent.getRecordType());
    }

    /**
     * Construct a plan for generating synthetic records from a record of a given stored record type.
     *
     * The generated records will derive from the stored record.
     *
     * Used when the record is updated to find synthetic records whose indexes need to be updated.
     * @param storedRecordType the stored record type
     * @param onlyIfIndexed only include synthetic types on which indexes are defined
     * @return a plan that can be applied to a record of the given type to generate synthetic records or {@code null} if
     * no indexed synthetic types include the given stored record type.
     */
    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public SyntheticRecordFromStoredRecordPlan fromStoredType(@Nonnull RecordType storedRecordType, boolean onlyIfIndexed) {
        if (storedRecordType.getRecordMetaData() != recordMetaData) {
            throw mismatchedMetaData();
        }
        if (storedRecordType.isSynthetic()) {
            throw new RecordCoreArgumentException("Record type is not a stored record type");
        }
        List<SyntheticRecordFromStoredRecordPlan> subPlans = new ArrayList<>();
        Set<String> syntheticRecordTypes = new HashSet<>();
        boolean needDistinct = false;
        for (SyntheticRecordType<?> syntheticRecordType : recordMetaData.getSyntheticRecordTypes().values()) {
            if (onlyIfIndexed && allIndexesDisabled(syntheticRecordType)) {
                continue;
            }
            if (timer != null) {
                timer.increment(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE);
            }
            for (SyntheticRecordType.Constituent constituent : syntheticRecordType.getConstituents()) {
                if (constituent.getRecordType() == storedRecordType) {
                    final SyntheticRecordFromStoredRecordPlan subPlan;
                    if (syntheticRecordType instanceof JoinedRecordType) {
                        subPlan = forJoinConstituent((JoinedRecordType)syntheticRecordType, (JoinedRecordType.JoinConstituent)constituent);
                    } else if (syntheticRecordType instanceof UnnestedRecordType) {
                        subPlan = forUnnestedConstituent((UnnestedRecordType) syntheticRecordType, (UnnestedRecordType.NestedConstituent) constituent);
                    } else {
                        throw unknownSyntheticType(syntheticRecordType);
                    }
                    subPlans.add(subPlan);
                    if (!syntheticRecordTypes.add(syntheticRecordType.getName())) {
                        // There is more than one path to some synthetic record type (e.g., self-join), so might be dupes.
                        needDistinct = true;
                    }
                }
            }
        }
        if (subPlans.isEmpty()) {
            return null;
        } else if (subPlans.size() == 1) {
            return subPlans.get(0);
        } else {
            return new SyntheticRecordConcatPlan(subPlans, needDistinct);
        }
    }

    private boolean allIndexesDisabled(SyntheticRecordType<?> syntheticRecordType) {
        return allIndexesDisabled(syntheticRecordType.getIndexes()) && allIndexesDisabled(syntheticRecordType.getMultiTypeIndexes());
    }

    private boolean allIndexesDisabled(@Nonnull Collection<Index> indexes) {
        if (indexes.isEmpty()) {
            return true;
        }
        return indexes.stream().allMatch(index -> queryPlanner.getRecordStoreState().isDisabled(index));
    }

    /**
     * Determine what stored record types would be need to scanned in order to rebuild a given index.
     *
     * From those scans, queries will be executed to load other record types to complete the synthesis.
     * <p>
     * In cases such as full outer join, there is no single record type from which all joins can be produced.
     * @param index the index that needs to be built
     * @param recordTypes a subset of the index's record types or {@code null} for all
     * @return a set of stored record types that are sufficient to generate the synthesized records for the index
     */
    public Set<RecordType> storedRecordTypesForIndex(@Nonnull Index index, @Nullable Collection<RecordType> recordTypes) {
        if (recordTypes == null) {
            recordTypes = recordMetaData.recordTypesForIndex(index);
        }
        Set<RecordType> result = new HashSet<>();
        for (RecordType recordType : recordTypes) {
            if (recordType instanceof JoinedRecordType) {
                JoinedRecordType joinedRecordType = (JoinedRecordType)recordType;
                Optional<JoinedRecordType.JoinConstituent> maybeConstituent = joinedRecordType.getConstituents().stream().filter(c -> !c.isOuterJoined()).findFirst();
                if (maybeConstituent.isPresent()) {
                    result.add(maybeConstituent.get().getRecordType());
                } else {
                    for (JoinedRecordType.JoinConstituent joinConstituent : joinedRecordType.getConstituents()) {
                        result.add(joinConstituent.getRecordType());
                    }
                }
            } else if (recordType instanceof UnnestedRecordType) {
                // In an unnested record type, only the parent provides a real record
                UnnestedRecordType unnestedRecordType = (UnnestedRecordType)recordType;
                result.add(unnestedRecordType.getParentConstituent().getRecordType());
            } else {
                throw unknownSyntheticType(recordType);
            }
        }
        return result;
    }

    /**
     * Construct a plan for generating synthetic records for a given index.
     *
     * The generated records will be of indexed record types.
     *
     * Used by the {@link com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer} to build from a full scan of stored records.
     * @param index an index on synthetic record types
     * @return a plan that can be applied to scanned records to generate synthetic records
     */
    @Nonnull
    public SyntheticRecordFromStoredRecordPlan forIndex(@Nonnull Index index) {
        final Collection<RecordType> recordTypes = recordMetaData.recordTypesForIndex(index);
        if (recordTypes.size() == 1) {
            final RecordType recordType = recordTypes.iterator().next();
            if (!recordType.isSynthetic()) {
                throw new RecordCoreException("Index does not apply to synthetic record types " + index);
            }
            return forType((SyntheticRecordType<?>)recordType);
        }
        Multimap<String, SyntheticRecordFromStoredRecordPlan> byType = ArrayListMultimap.create();
        for (RecordType recordType : recordTypes) {
            if (!(recordType instanceof JoinedRecordType)) {
                throw unknownSyntheticType(recordType);
            }
            JoinedRecordType joinedRecordType = (JoinedRecordType)recordType;
            Optional<JoinedRecordType.JoinConstituent> maybeConstituent = joinedRecordType.getConstituents().stream().filter(c -> !c.isOuterJoined()).findFirst();
            if (maybeConstituent.isPresent()) {
                addToByType(byType, joinedRecordType, maybeConstituent.get());
            } else {
                for (JoinedRecordType.JoinConstituent joinConstituent : joinedRecordType.getConstituents()) {
                    addToByType(byType, joinedRecordType, joinConstituent);
                }
            }
        }
        return createByType(byType);
    }

    /**
     * Construct a plan for generating synthetic records from a constituent of a joined record type.
     *
     * The generated records will start the joins from this constituent.
     * @param joinedRecordType the joined record type
     * @param joinConstituent the constituent type
     * @return a plan that can be applied to a record of the given type to generate synthetic records
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public SyntheticRecordFromStoredRecordPlan forJoinConstituent(@Nonnull JoinedRecordType joinedRecordType,
                                                                  @Nonnull JoinedRecordType.JoinConstituent joinConstituent) {
        if (joinedRecordType.getRecordMetaData() != recordMetaData) {
            throw mismatchedMetaData();
        }
        if (!joinedRecordType.getConstituents().contains(joinConstituent)) {
            throw new RecordCoreArgumentException("Join constituent is not from record type");
        }
        return new JoinedRecordPlanner(joinedRecordType, queryPlanner).plan(joinConstituent);
    }

    /**
     * Construct a plan for generating synthetic records from the parent constituent of an unnested record type.
     * This will un-nest nested constituents from the stored record and construct synthetic records joining the
     * parent record with its named nested elements.
     *
     * @param unnestedRecordType the unnested record type
     * @param constituent the parent constituent of the unnested record type
     * @return a plan that generates synthetic records from un-nesting stored records
     * @see UnnestedRecordType
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // want pointer equality for record meta-data object
    public SyntheticRecordFromStoredRecordPlan forUnnestedConstituent(@Nonnull UnnestedRecordType unnestedRecordType,
                                                                      @Nonnull UnnestedRecordType.NestedConstituent constituent) {
        if (unnestedRecordType.getRecordMetaData() != recordMetaData) {
            throw mismatchedMetaData();
        }
        if (!unnestedRecordType.getConstituents().contains(constituent)) {
            throw new RecordCoreArgumentException("Constituent is not from record type");
        }
        return new UnnestedRecordPlanner(unnestedRecordType).plan(constituent);
    }

    private void addToByType(@Nonnull Multimap<String, SyntheticRecordFromStoredRecordPlan> byType,
                             @Nonnull JoinedRecordType joinedRecordType, @Nonnull JoinedRecordType.JoinConstituent joinConstituent) {
        byType.put(joinConstituent.getRecordType().getName(), forJoinConstituent(joinedRecordType, joinConstituent));
    }

    @Nonnull
    private SyntheticRecordByTypePlan createByType(@Nonnull Multimap<String, SyntheticRecordFromStoredRecordPlan> byType) {
        Map<String, SyntheticRecordFromStoredRecordPlan> map = new HashMap<>();
        for (Map.Entry<String, Collection<SyntheticRecordFromStoredRecordPlan>> entry : byType.asMap().entrySet()) {
            map.put(entry.getKey(), entry.getValue().size() == 1 ?
                                    entry.getValue().iterator().next() :
                                    new SyntheticRecordConcatPlan(new ArrayList<>(entry.getValue()), false));
        }
        return new SyntheticRecordByTypePlan(map);
    }

    static RecordCoreException mismatchedMetaData() {
        return new RecordCoreArgumentException("Record type does not belong to same meta-data");
    }

    static RecordCoreException unknownSyntheticType(@Nonnull RecordType syntheticRecordType) {
        return new RecordCoreException("Do not know how to generate synthetic records for " + syntheticRecordType);
    }

}
