/*
 * QueryCacheKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * This is used to look up a plan in the primary cache (see {@link MultiStageCache} for more information).
 * It comprises the following fields:
 * <ul>
 *     <li>The schema template name to which the query is bound. It is necessary to use it so we can segregate
 *     the otherwise identical plans but coming from different schemas (see Example 1 below)</li>
 *     <li>The schema template version, user version, and a bit-set of all readable indexes</li>
 *     <li>The canonical query string where all literals are removed and white spaces are normalised (see example 2)</li>
 *     <li>The hash of the query, see {@link AstNormalizer} for more information on how is this generated.</li>
 * </ul>
 * <b>Example1</b>
 * <br>
 * Let us assume we have two schema templates ({@code s1} and {@code s2}) defined as the following:
 * <pre>
 * {@code
 * create schema template s1
 * create table t1(id bigint, col1 bigint, primary key(id))
 *
 * create schema template s2
 * create table t1(id bigint, col1 bigint, col3 bigint, primary key(id))
 * }
 * </pre>
 * If we run a query like this:
 * <pre>
 * {@code
 * create schema /FRL/YOUSSEF/s1s with s1
 * connect: "jdbc:embed:/FRL/YOUSSEF?schema=S1S"
 * select * from t1 where col1 > 42;
 * }
 * </pre>
 * we would compile the query and return a result set comprising two columns {@code id, col1}.
 * we will also cache this query using a {@link QueryCacheKey} key of something like {@code "s1", "select * from t1 where col1 > ? ", 123456789}
 * <br>
 * if we run the <i>same</i> query, however after connecting to a schema that uses a {@code s2} instead:
 * <pre>
 * {@code
 * create schema /FRL/YOUSSEF/s2s with s2
 * connect: "jdbc:embed:/FRL/YOUSSEF?schema=S2S"
 * select * from t1 where col1 > 53;
 * }
 * </pre>
 * we would correctly compile this query and return a result set comprising three columns {@code id, col1, col2}.
 * we will also cache this query using {@link QueryCacheKey} key of something like {@code "s2", "select * from t1 where col1 > ? ", 123456789}
 * <br>
 * Note that without having the schema template name as part of the {@link QueryCacheKey} both keys would be identical.
 * Therefore, we might incorrectly choose the compiled plan of the first query to execute the second query (because we find
 * a match in the cache) causing an error since the result set structure is different because table {@code T1} is defined
 * differently in {@code S1} and {@code S2}.
 * <br>
 * <br>
 * <b>Example 2</b>
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

    private final int hash;

    private final int schemaTemplateVersion;

    private final int userVersion;

    private final int memoizedHashCode;

    private QueryCacheKey(@Nonnull final String canonicalQueryString,
                          @Nonnull final PlannerConfiguration plannerConfiguration,
                          @Nonnull final String auxiliaryMetadata,
                          int hash,
                          int schemaTemplateVersion,
                          int userVersion) {
        this.canonicalQueryString = canonicalQueryString;
        this.hash = hash;
        this.schemaTemplateVersion = schemaTemplateVersion;
        this.userVersion = userVersion;
        this.auxiliaryMetadata = auxiliaryMetadata;
        this.plannerConfiguration = plannerConfiguration;

        // Memoize the hash code. Because this object is used as a key in a hash map, it is important that
        // hashCode() be quick. Note that this includes information about the query (like the query hash),
        // the schema template version, and the schema (like the set of readable indexes)
        this.memoizedHashCode = Objects.hash(hash, schemaTemplateVersion, plannerConfiguration, userVersion, auxiliaryMetadata);
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
        return hash == that.hash &&
                schemaTemplateVersion == that.schemaTemplateVersion &&
                userVersion == that.userVersion &&
                Objects.equals(canonicalQueryString, that.canonicalQueryString) &&
                Objects.equals(auxiliaryMetadata, that.auxiliaryMetadata) &&
                Objects.equals(plannerConfiguration, that.plannerConfiguration);
    }

    @Override
    public int hashCode() {
        return memoizedHashCode;
    }

    public int getHash() {
        return hash;
    }

    @Nonnull
    public String getCanonicalQueryString() {
        return canonicalQueryString;
    }

    public int getSchemaTemplateVersion() {
        return schemaTemplateVersion;
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
        return "(" + schemaTemplateVersion + " || " + auxiliaryMetadata + ")" + "||" + canonicalQueryString + "||" + hash;
    }

    @Nonnull
    public static QueryCacheKey of(@Nonnull final String query,
                                   @Nonnull final PlannerConfiguration plannerConfiguration,
                                   @Nonnull final String auxiliaryMetadata,
                                   int queryHash,
                                   int schemaTemplateVersion,
                                   int userVersion) {
        return new QueryCacheKey(query, plannerConfiguration, auxiliaryMetadata, queryHash, schemaTemplateVersion,
                userVersion);
    }
}
