/*
 * DatabaseObjectsDependencies.java
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
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PDatabaseObjectDependenciesPredicate;
import com.apple.foundationdb.record.planprotos.PDatabaseObjectDependenciesPredicate.PUsedIndex;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A predicate to be used as part of a {@link com.apple.foundationdb.record.query.plan.QueryPlanConstraint}
 * which can determine if a given plan can be executed under the currently available database objects.
 */
@API(API.Status.EXPERIMENTAL)
public class DatabaseObjectDependenciesPredicate extends AbstractQueryPredicate implements LeafQueryPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Database-Object-Dependencies-Predicate");

    @Nonnull
    private final Set<UsedIndex> usedIndexes;

    /**
     * Constructs a new {@link DatabaseObjectDependenciesPredicate} instance.
     * @param usedIndexes a set of used indexes
     */
    private DatabaseObjectDependenciesPredicate(@Nonnull final Set<UsedIndex> usedIndexes) {
        super(true);
        this.usedIndexes = ImmutableSet.copyOf(usedIndexes);
    }

    @Nonnull
    public Set<UsedIndex> getUsedIndexes() {
        return usedIndexes;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context) {
        final RecordMetaData recordMetaData = store.getRecordMetaData();
        for (final UsedIndex usedIndex : usedIndexes) {
            if (!recordMetaData.hasIndex(usedIndex.getName())) {
                return false;
            }
            final Index currentIndex = recordMetaData.getIndex(usedIndex.getName());
            if (usedIndex.getLastModifiedVersion() != currentIndex.getLastModifiedVersion()) {
                return false;
            }
            final RecordStoreState recordStoreState = store.getRecordStoreState();
            if (!recordStoreState.isReadable(currentIndex)) {
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
                super.hashCodeWithoutChildren(), usedIndexes);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, isAtomic(), usedIndexes);
    }

    @Override
    public String toString() {
        return "databaseObjectDependencies()";
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
                    final DatabaseObjectDependenciesPredicate otherDatabaseObjectDependenciesPredicate =
                            (DatabaseObjectDependenciesPredicate)other;
                    return usedIndexes.equals(otherDatabaseObjectDependenciesPredicate.usedIndexes);
                });
    }


    @Nonnull
    @Override
    public PDatabaseObjectDependenciesPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PDatabaseObjectDependenciesPredicate.Builder builder = PDatabaseObjectDependenciesPredicate.newBuilder();
        for (final UsedIndex usedIndex : usedIndexes) {
            builder.addUsedIndexes(usedIndex.toProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setDatabaseObjectDependenciesPredicate(toProto(serializationContext)).build();
    }

    @Nonnull
    public static DatabaseObjectDependenciesPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                @Nonnull final PDatabaseObjectDependenciesPredicate databaseObjectDependenciesPredicateProto) {
        final ImmutableSet.Builder<UsedIndex> usedIndexesBuilder = ImmutableSet.builder();
        for (int i = 0; i < databaseObjectDependenciesPredicateProto.getUsedIndexesCount(); i ++) {
            final PUsedIndex usedIndexProto = databaseObjectDependenciesPredicateProto.getUsedIndexes(i);
            usedIndexesBuilder.add(UsedIndex.fromProto(serializationContext, usedIndexProto));
        }
        return new DatabaseObjectDependenciesPredicate(usedIndexesBuilder.build());
    }

    @Nonnull
    public static DatabaseObjectDependenciesPredicate fromPlan(@Nonnull final RecordMetaData recordMetaData,
                                                               @Nonnull final RecordQueryPlan plan) {
        final List<String> usedIndexesNamesList = Lists.newArrayList(plan.getUsedIndexes());
        // we have to do this to get a proper stable order
        Collections.sort(usedIndexesNamesList);
        final ImmutableSet.Builder<UsedIndex> usedIndexesBuilder = ImmutableSet.builder();
        for (final String usedIndexName : usedIndexesNamesList) {
            final Index index = recordMetaData.getIndex(usedIndexName);
            usedIndexesBuilder.add(new UsedIndex(usedIndexName, index.getLastModifiedVersion()));
        }
        return new DatabaseObjectDependenciesPredicate(usedIndexesBuilder.build());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PDatabaseObjectDependenciesPredicate, DatabaseObjectDependenciesPredicate> {
        @Nonnull
        @Override
        public Class<PDatabaseObjectDependenciesPredicate> getProtoMessageClass() {
            return PDatabaseObjectDependenciesPredicate.class;
        }

        @Nonnull
        @Override
        public DatabaseObjectDependenciesPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                             @Nonnull final PDatabaseObjectDependenciesPredicate databaseObjectDependenciesPredicateProto) {
            return DatabaseObjectDependenciesPredicate.fromProto(serializationContext, databaseObjectDependenciesPredicateProto);
        }
    }

    /**
     * Helper class to capture the name together and the last modified version of an index.
     */
    public static class UsedIndex implements PlanHashable, PlanSerializable {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Used-Index");

        @Nonnull
        private final String name;
        private final int lastModifiedVersion;

        public UsedIndex(@Nonnull final String name, final int lastModifiedVersion) {
            this.name = name;
            this.lastModifiedVersion = lastModifiedVersion;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        public int getLastModifiedVersion() {
            return lastModifiedVersion;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UsedIndex)) {
                return false;
            }
            final UsedIndex usedIndex = (UsedIndex)o;
            return lastModifiedVersion == usedIndex.lastModifiedVersion &&
                    Objects.equals(name, usedIndex.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, lastModifiedVersion);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, name, lastModifiedVersion);
        }

        @Nonnull
        @Override
        public PUsedIndex toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PUsedIndex.newBuilder()
                    .setName(name)
                    .setLastModifiedVersion(lastModifiedVersion)
                    .build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static UsedIndex fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PUsedIndex usedIndexProto) {
            return new UsedIndex(usedIndexProto.getName(), usedIndexProto.getLastModifiedVersion());
        }
    }
}
