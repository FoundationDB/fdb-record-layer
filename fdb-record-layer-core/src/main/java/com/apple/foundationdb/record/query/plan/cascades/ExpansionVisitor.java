/*
 * KeyExpressionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A sub interface of {@link KeyExpressionVisitor} that fixes the return type to be a {@link GraphExpansion} and
 * adds an API to cause the expansion of a metadata-based data access structure to a data flow graph that can then
 * be used for matching.
 * @param <S> the type of the state object whose actual type depends on the implementation of the concrete visitor
 */
@SuppressWarnings("java:S3252")
public interface ExpansionVisitor<S extends KeyExpressionVisitor.State> extends KeyExpressionVisitor<S, GraphExpansion> {
    /**
     * Method that expands a data structure into a data flow graph. The expansion creates a base reference from
     * the given record type information and access hint, then visits the underlying key expression to produce
     * a {@link MatchCandidate} that can be used for index matching during planning.
     *
     * @param availableRecordTypeNames the set of all record type names available in the meta-data
     * @param queriedRecordTypeNames the subset of record type names being queried
     * @param baseType the record type representing the common fields across the queried record types
     * @param accessHint the access hint indicating the desired scan type (e.g., index scan, primary scan)
     * @param primaryKey the primary key of the data object the caller wants to access
     * @param isReverse an indicator whether the result set is expected to be returned in reverse order
     *
     * @return a new {@link MatchCandidate} that can be used for matching.
     */
    @Nonnull
    MatchCandidate expand(@Nonnull Set<String> availableRecordTypeNames,
                          @Nonnull Set<String> queriedRecordTypeNames,
                          @Nonnull Type.Record baseType,
                          @Nonnull AccessHint accessHint,
                          @Nullable KeyExpression primaryKey,
                          boolean isReverse);

    /**
     * Alternative expand method that uses a pre-built base quantifier supplier instead of constructing
     * the base reference from record type names. This overload exists to support windowed indexes (rank indexes)
     * which require a custom base data access pattern. It should be removed once we migrate to the new
     * representation of windowed indexes (see
     * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/4039">issue #4039</a>).
     * The default implementation throws {@link UnsupportedOperationException}.
     *
     * @param baseQuantifierSupplier a quantifier supplier to create base data access
     * @param primaryKey the primary key of the data object the caller wants to access
     * @param isReverse an indicator whether the result set is expected to be returned in reverse order
     *
     * @return a new {@link MatchCandidate} that can be used for matching.
     * @throws UnsupportedOperationException if the visitor does not support this expand overload
     */
    @Nonnull
    default MatchCandidate expand(@Nonnull Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                          @Nullable KeyExpression primaryKey,
                          boolean isReverse) {
        throw new UnsupportedOperationException("expansion with base quantifier supplier is not supported");
    }

    /**
     * Creates a base reference consisting of a full unordered scan filtered by the queried record types,
     * used as the starting point for index and value expansion.
     *
     * @param availableRecordTypeNames the set of all record type names available in the meta-data
     * @param queriedRecordTypeNames the subset of record type names being queried
     * @param baseType the record type representing the common fields across the queried record types
     * @param recordTypeKeyAlias an optional alias for the record type key, or {@code null} if not applicable
     * @param accessHint the access hint indicating the desired scan type (e.g., index scan, primary scan)
     * @return a new {@link Reference} wrapping a type-filtered full unordered scan
     */
    @Nonnull
    static Reference createBaseRef(@Nonnull final Set<String> availableRecordTypeNames,
                                   @Nonnull final Set<String> queriedRecordTypeNames,
                                   @Nonnull final Type.Record baseType,
                                   @Nullable final CorrelationIdentifier recordTypeKeyAlias,
                                   @Nonnull AccessHint accessHint) {
        final var quantifier =
                Quantifier.forEach(
                        Reference.initialOf(
                                new FullUnorderedScanExpression(availableRecordTypeNames,
                                        new Type.AnyRecord(false),
                                        new AccessHints(accessHint))));
        return Reference.initialOf(LogicalTypeFilterExpression.newInstanceForMatchCandidate(queriedRecordTypeNames,
                quantifier, baseType, recordTypeKeyAlias));
    }
}
