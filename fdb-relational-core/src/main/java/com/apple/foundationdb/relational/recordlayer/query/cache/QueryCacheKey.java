/*
 * QueryCacheKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.recordlayer.query.AstNormalizer;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.google.common.collect.ImmutableSortedMap;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * This is used to look up a plan in the primary cache (see {@link MultiStageCache} for more information).
 * It comprises the following fields:
 * <ul>
 *     <li>The schema template versions to which the query is bound, keyed by schema template name. A plan is
 *     invalidated when <em>any</em> participating schema bumps its version.</li>
 *     <li>The user version and a bit-set of all readable indexes.</li>
 *     <li>The canonical query string where all literals are removed and white spaces are normalised (see example below).</li>
 *     <li>The hash of the query, see {@link AstNormalizer} for more information on how is this generated.</li>
 * </ul>
 * <b>Example</b>
 * <br>
 * Although these queries appear different, their canonical representation is the same, and will end up using the same
 * compiled plan.
 * <br>
 * <table>
 *     <caption>queries with identical canonical representation</caption>
 *     <tbody>
 *         <tr><td>select a, b, ? from t1 where col1 &gt; 42</td></tr>
 *         <tr><td>select a, b, ? from   t1 where col1 &gt; 42</td></tr>
 *         <tr><td>select a, b, ? from t1 where col1 &gt; 50</td></tr>
 *         <tr><td>select a, b, ? from  t1 where col1 &gt; 50 with continuation FFEE</td></tr>
 *         <tr><td>select a, b, 54  \t\t\t\n\n\n from t1 where col1 &gt; 100</td></tr>
 *         <tr><td>select a, b, ? from t1 where col &gt; 100 limit 10</td></tr>
 *     </tbody>
 * </table>
 * For more information, examine the unit tests of {@link AstNormalizer}.
 */
@API(API.Status.EXPERIMENTAL)
public final class QueryCacheKey {

    @Nonnull
    private final String canonicalQueryString;

    @Nonnull
    private final PlannerConfiguration plannerConfiguration;

    @Nonnull
    private final String auxiliaryMetadata;

    /**
     * Maps each participating schema template name to its version. A cache miss is triggered when
     * any entry in this map changes, which is the correct behaviour for cross-schema queries.
     */
    @Nonnull
    private final ImmutableSortedMap<String, Integer> schemaVersions;

    private final int userVersion;

    private final int memoizedHashCode;

    private QueryCacheKey(@Nonnull final String canonicalQueryString,
                          @Nonnull final PlannerConfiguration plannerConfiguration,
                          @Nonnull final String auxiliaryMetadata,
                          @Nonnull final ImmutableSortedMap<String, Integer> schemaVersions,
                          int userVersion) {
        this.canonicalQueryString = canonicalQueryString;
        this.schemaVersions = schemaVersions;
        this.userVersion = userVersion;
        this.auxiliaryMetadata = auxiliaryMetadata;
        this.plannerConfiguration = plannerConfiguration;

        this.memoizedHashCode = Objects.hash(canonicalQueryString, schemaVersions, plannerConfiguration, userVersion, auxiliaryMetadata);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final var that = (QueryCacheKey) other;
        return userVersion == that.userVersion &&
                Objects.equals(canonicalQueryString, that.canonicalQueryString) &&
                Objects.equals(auxiliaryMetadata, that.auxiliaryMetadata) &&
                Objects.equals(plannerConfiguration, that.plannerConfiguration) &&
                Objects.equals(schemaVersions, that.schemaVersions);
    }

    @Override
    public int hashCode() {
        return memoizedHashCode;
    }

    @Nonnull
    public String getCanonicalQueryString() {
        return canonicalQueryString;
    }

    @Nonnull
    public ImmutableSortedMap<String, Integer> getSchemaVersions() {
        return schemaVersions;
    }

    @Nonnull
    public PlannerConfiguration getPlannerConfiguration() {
        return plannerConfiguration;
    }

    public int getUserVersion() {
        return userVersion;
    }

    @Nonnull
    public String getAuxiliaryMetadata() {
        return auxiliaryMetadata;
    }

    @Override
    public String toString() {
        return "(" + schemaVersions + " || " + auxiliaryMetadata + ")" + "||" + canonicalQueryString + "||" + memoizedHashCode;
    }

    @Nonnull
    public static QueryCacheKey of(@Nonnull final String query,
                                   @Nonnull final PlannerConfiguration plannerConfiguration,
                                   @Nonnull final String auxiliaryMetadata,
                                   @Nonnull final ImmutableSortedMap<String, Integer> schemaVersions,
                                   int userVersion) {
        return new QueryCacheKey(query, plannerConfiguration, auxiliaryMetadata, schemaVersions, userVersion);
    }
}
