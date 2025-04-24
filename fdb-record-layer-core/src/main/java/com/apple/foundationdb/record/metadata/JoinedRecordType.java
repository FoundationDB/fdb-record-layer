/*
 * JoinedRecordType.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.RecordDoesNotExistException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A <i>synthetic</i> record type representing the indexable result of <em>joining</em> stored records.
 * <p>
 * Joined record represents a collection of <i>joined constituents</i>, each represented by another record in the database.
 * The constituents are joined together by a <i>join condition</i>: in this case, a set of fields that are tested for
 * equality across all constituents.
 * The joined record primary key is represented by a Tuple as follows:
 * <pre>
 *     [{record type}, {elements of constituent 1 PK}, ... {elements of constituent n PK}]
 * </pre>
 * When loading a joined record, the joined record primary key is used to iterate and load each constituent, eventually
 * combining the collection of constituents into the completed record.
 * <p>
 * There is nothing special needed to be done to save a joined record. Once the constituent records are saved
 * a joined record instance is implicitly assumed to exist. A subsequent {@link #loadByPrimaryKeyAsync(FDBRecordStore, Tuple, IndexOrphanBehavior)}
 * or a query for the record or a scan of a joined index will create the joined record. similarly, a deletion
 * of any or all of the constituents will implicitly render the joined record type deleted.
 * <p>
 * When loading a joined record (and similarly when scanning an index) an {@link IndexOrphanBehavior} can be used
 * to determine the behavior in case one or more of the constituents are missing:
 * <ul>
 *     <li>{@link IndexOrphanBehavior#ERROR} (the default) will throw an exception</li>
 *     <li>{@link IndexOrphanBehavior#RETURN} will return an instance of the records with no constituents</li>
 *     <li>{@link IndexOrphanBehavior#SKIP} will return null</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class JoinedRecordType extends SyntheticRecordType<JoinedRecordType.JoinConstituent> {

    @Nonnull
    private final List<Join> joins;

    /**
     * A constituent type within a joined record type.
     */
    public static class JoinConstituent extends SyntheticRecordType.Constituent {
        private final boolean outerJoined;

        protected JoinConstituent(String name, RecordType recordType, boolean outerJoined) {
            super(name, recordType);
            this.outerJoined = outerJoined;
        }

        public boolean isOuterJoined() {
            return outerJoined;
        }
    }

    /**
     * An equi-join between constituent types.
     */
    public static class Join {
        @Nonnull
        private final JoinConstituent left;
        @Nonnull
        private final KeyExpression leftExpression;
        @Nonnull
        private final JoinConstituent right;
        @Nonnull
        private final KeyExpression rightExpression;

        protected Join(@Nonnull JoinConstituent left, @Nonnull KeyExpression leftExpression,
                       @Nonnull JoinConstituent right, @Nonnull KeyExpression rightExpression) {
            this.left = left;
            this.leftExpression = leftExpression;
            this.right = right;
            this.rightExpression = rightExpression;
        }

        @Nonnull
        public JoinConstituent getLeft() {
            return left;
        }

        @Nonnull
        public KeyExpression getLeftExpression() {
            return leftExpression;
        }

        @Nonnull
        public JoinConstituent getRight() {
            return right;
        }

        @Nonnull
        public KeyExpression getRightExpression() {
            return rightExpression;
        }
    }

    @SuppressWarnings("squid:S00107") // Comes from Builder.
    protected JoinedRecordType(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.Descriptor descriptor,
                               @Nonnull KeyExpression primaryKey, @Nonnull Object recordTypeKey,
                               @Nonnull List<Index> indexes, @Nonnull List<Index> multiTypeIndexes,
                               @Nonnull List<JoinConstituent> constituents, @Nonnull List<Join> joins) {
        super(metaData, descriptor, primaryKey, recordTypeKey, indexes, multiTypeIndexes, constituents);
        this.joins = joins;
    }

    @Nonnull
    public List<Join> getJoins() {
        return joins;
    }

    @Nonnull
    @Override
    @API(API.Status.INTERNAL)
    public CompletableFuture<FDBSyntheticRecord> loadByPrimaryKeyAsync(FDBRecordStore store, Tuple primaryKey, IndexOrphanBehavior orphanBehavior) {
        int nconstituents = getConstituents().size();
        final Map<String, FDBStoredRecord<? extends Message>> constituentValues = new ConcurrentHashMap<>(nconstituents);
        final CompletableFuture<?>[] futures = new CompletableFuture<?>[nconstituents];
        AtomicBoolean isMissingConstituent = new AtomicBoolean(false);
        for (int i = 0; i < nconstituents; i++) {
            final SyntheticRecordType.Constituent constituent = getConstituents().get(i);
            final Tuple constituentKey = primaryKey.getNestedTuple(i + 1);
            if (constituentKey == null) {
                futures[i] = AsyncUtil.DONE;
            } else {
                futures[i] = store.loadRecordAsync(constituentKey).thenApply(rec -> {
                    if (rec == null) {
                        if (orphanBehavior.equals(IndexOrphanBehavior.ERROR)) {
                            throw new RecordDoesNotExistException("constituent record not found: " + constituent.getName());
                        } else {
                            // For SKIP and RETURN
                            isMissingConstituent.set(true);
                            // ideally, we should be able to stop the iteration to fetch all other constituents
                            // but because of the async nature of the loop this seems to be not worth it
                        }
                    } else {
                        constituentValues.put(constituent.getName(), rec);
                    }
                    return null;
                });
            }
        }
        return CompletableFuture.allOf(futures).thenApply(vignore -> {
            if ( ! isMissingConstituent.get()) {
                // all constituents have been found
                return FDBSyntheticRecord.of(this, constituentValues);
            } else {
                // some constituents are missing
                if (orphanBehavior.equals(IndexOrphanBehavior.SKIP)) {
                    return null;
                } else {
                    // This is for RETURN - return the shell of the record with no constituents
                    return FDBSyntheticRecord.of(this, Map.of());
                }
            }
        });
    }

    @Nonnull
    public RecordMetaDataProto.JoinedRecordType toProto() {
        RecordMetaDataProto.JoinedRecordType.Builder typeBuilder = RecordMetaDataProto.JoinedRecordType.newBuilder()
                .setName(getName())
                .setRecordTypeKey(LiteralKeyExpression.toProtoValue(getRecordTypeKey()));
        for (JoinedRecordType.JoinConstituent joinConstituent : getConstituents()) {
            RecordMetaDataProto.JoinedRecordType.JoinConstituent.Builder constituentBuilder = typeBuilder.addJoinConstituentsBuilder()
                    .setName(joinConstituent.getName())
                    .setRecordType(joinConstituent.getRecordType().getName());
            if (joinConstituent.isOuterJoined()) {
                constituentBuilder.setOuterJoined(true);
            }
        }
        for (JoinedRecordType.Join join : getJoins()) {
            typeBuilder.addJoinsBuilder()
                    .setLeft(join.getLeft().getName())
                    .setLeftExpression(join.getLeftExpression().toKeyExpression())
                    .setRight(join.getRight().getName())
                    .setRightExpression(join.getRightExpression().toKeyExpression());
        }
        return typeBuilder.build();
    }

}
