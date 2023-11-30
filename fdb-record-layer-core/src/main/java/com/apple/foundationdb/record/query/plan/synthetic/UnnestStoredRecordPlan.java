/*
 * UnnestStoredRecordPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan that takes a stored record and explodes out nested repeated elements as specified by the constituents
 * of an {@link UnnestedRecordType}. Each {@link UnnestedRecordType} should have a single stored record, and
 * then the other constituents can be constructed by exploding nested repeated elements (as specified by the
 * {@linkplain UnnestedRecordType.NestedConstituent#getNestingExpression() nesting expression} on the
 * constituent). This will produce a cursor that produces one element for each such un-nesting.
 */
@API(API.Status.INTERNAL)
class UnnestStoredRecordPlan implements SyntheticRecordFromStoredRecordPlan {
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("UnnestStoredRecordPlan");

    @Nonnull
    private final UnnestedRecordType recordType;
    @Nonnull
    private final RecordType storedRecordType;

    UnnestStoredRecordPlan(@Nonnull UnnestedRecordType recordType, @Nonnull RecordType storedRecordType) {
        this.recordType = recordType;
        this.storedRecordType = storedRecordType;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, recordType.getName(), storedRecordType.getName());
    }

    @Nonnull
    @Override
    public Set<String> getStoredRecordTypes() {
        return Collections.singleton(storedRecordType.getName());
    }

    @Nonnull
    @Override
    public Set<String> getSyntheticRecordTypes() {
        return Collections.singleton(recordType.getName());
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBSyntheticRecord> execute(@Nonnull final FDBRecordStore store,
                                                                        @Nonnull final FDBStoredRecord<M> rec,
                                                                        @Nullable final byte[] continuation,
                                                                        @Nonnull final ExecuteProperties executeProperties) {
        NestingNode root = new NestingNode(recordType.getParentConstituent(), rec);
        Deque<NestingNode> toProcess = new ArrayDeque<>();
        toProcess.add(root);
        while (!toProcess.isEmpty()) {
            NestingNode next = toProcess.pollFirst();
            for (UnnestedRecordType.NestedConstituent nesting : recordType.getConstituents()) {
                next.processNesting(nesting, toProcess);
            }
        }

        final List<FDBSyntheticRecord> resultRecords = iterateTree(root);
        return RecordCursor.fromList(store.getExecutor(), resultRecords);
    }

    private List<FDBSyntheticRecord> iterateTree(NestingNode root) {
        List<FDBSyntheticRecord> records = new ArrayList<>();
        Map<String, FDBStoredRecord<?>> constituentMap = Maps.newHashMapWithExpectedSize(recordType.getConstituents().size());
        root.initializeState(constituentMap);
        records.add(FDBSyntheticRecord.of(recordType, ImmutableMap.copyOf(constituentMap)));
        while (root.incrementState(constituentMap)) {
            records.add(FDBSyntheticRecord.of(recordType, ImmutableMap.copyOf(constituentMap)));
        }
        return records;
    }

    private static class NestingNode {
        @Nonnull
        private final UnnestedRecordType.NestedConstituent constituent;
        @Nonnull
        private final FDBStoredRecord<?> storedRecord;
        @Nullable
        private Map<String, List<NestingNode>> children;
        @Nullable
        private Map<String, Integer> state; // used to track which
        @Nullable
        private List<String> keys; // children map keys (stored in a list to ensure a stable ordering)

        public NestingNode(@Nonnull UnnestedRecordType.NestedConstituent constituent, @Nonnull FDBStoredRecord<?> storedRecord) {
            this.constituent = constituent;
            this.storedRecord = storedRecord;
        }

        public boolean processNesting(@Nonnull UnnestedRecordType.NestedConstituent nesting, final Deque<NestingNode> toProcess) {
            if (!constituent.getName().equals(nesting.getParentName())) {
                return false;
            }
            List<Key.Evaluated> evaluatedList = nesting.getNestingExpression().evaluate(storedRecord);
            for (int i = 0; i < evaluatedList.size(); i++) {
                Key.Evaluated evaluated = evaluatedList.get(i);
                Message childMessage = evaluated.getObject(0, Message.class);
                FDBStoredRecord<?> childRecord = FDBStoredRecord.newBuilder(childMessage)
                        .setRecordType(nesting.getRecordType())
                        .setPrimaryKey(Tuple.from(i))
                        .build();
                addChild(nesting, childRecord, toProcess);
            }
            return true;
        }

        private void addChild(UnnestedRecordType.NestedConstituent childConstituent, FDBStoredRecord<?> childRecord, Deque<NestingNode> toProcess) {
            if (children == null) {
                children = new HashMap<>();
            }
            final String childName = childConstituent.getName();
            final List<NestingNode> childList;
            if (children.containsKey(childName)) {
                childList = children.get(childName);
            } else {
                childList = new ArrayList<>();
                children.put(childName, childList);
                keys = null;
            }
            NestingNode newChild = new NestingNode(childConstituent, childRecord);
            childList.add(newChild);
            toProcess.addLast(newChild);
        }

        @Nonnull
        public List<String> getKeys() {
            if (children == null) {
                return Collections.emptyList();
            }
            if (keys == null) {
                List<String> keyList = new ArrayList<>(children.size());
                keyList.addAll(children.keySet());
                keyList.sort(Comparator.naturalOrder());
                keys = keyList;
            }
            return keys;
        }

        /**
         * Set up the state for this node. This will make sure this node's stored record is in the
         * constituent map, and it will ensure that all of the child constituents have been set up
         * to begin iterating through. This method should be called before {@link #incrementState(Map)}.
         *
         * @param constituentMap constituent map to populate with this node's value
         */
        public void initializeState(@Nonnull Map<String, FDBStoredRecord<?>> constituentMap) {
            constituentMap.put(constituent.getName(), storedRecord);
            if (children != null) {
                if (state == null) {
                    state = Maps.newHashMapWithExpectedSize(children.size());
                }
                for (String key : getKeys()) {
                    NestingNode child = children.get(key).get(0);
                    child.initializeState(constituentMap);
                    state.put(key, 0);
                }
            }
        }

        /**
         * Increments what ammounts to an an iterator on the tree. This iterator will populate the
         * {@code constituentMap} with a different value from the the tree. It does this by updating
         * the nodes in the tree. Once one subtree has been entirely iterated through, this continues
         * by incrementing the next one.
         *
         * @param constituentMap map to store the current constituens in for the updated state
         * @return whether this sub-node is done iterating
         */
        public boolean incrementState(@Nonnull Map<String, FDBStoredRecord<?>> constituentMap) {
            if (children == null) {
                // No children. In this case, there is only one state
                return false;
            }
            if (state == null) {
                throw new RecordCoreException("cannot increment un-initialized state");
            }
            // Go through the children in key-order. Keep going until we reach a child
            // which returns true (indicating that we have not exhausted that subtree).
            // This can happen either because of one of the current constituents has
            // children (that have not been exhausted) or because we have another child
            // for that constituent. If all of the child constituents have been exhausted,
            // we are done here
            for (String key : getKeys()) {
                int pos = state.get(key);
                List<NestingNode> keyChildren = children.get(key);
                NestingNode child = keyChildren.get(pos);
                if (child.incrementState(constituentMap)) {
                    return true;
                } else if (pos + 1 < keyChildren.size()) {
                    state.put(key, pos + 1);
                    NestingNode newChild = keyChildren.get(pos + 1);
                    newChild.initializeState(constituentMap);
                    return true;
                } else {
                    state.put(key, 0);
                    keyChildren.get(0).initializeState(constituentMap);
                }
            }
            return false;
        }
    }
}
