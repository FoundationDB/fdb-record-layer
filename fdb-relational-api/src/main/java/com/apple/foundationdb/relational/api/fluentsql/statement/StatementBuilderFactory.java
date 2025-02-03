/*
 * StatementBuilderFactory.java
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

package com.apple.foundationdb.relational.api.fluentsql.statement;

import com.apple.foundationdb.relational.api.ParseTreeInfo;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public interface StatementBuilderFactory {

    @Nonnull
    UpdateStatement.Builder updateStatementBuilder();

    @Nonnull
    UpdateStatement.Builder updateStatementBuilder(@Nonnull String updateQuery);

    /**
     * Generates an {@link UpdateStatement.Builder} from a given SQL statement using the provided map of column synonyms.
     *
     * @param updateQuery The update query.
     * @param columnSynonyms A map of column synonyms (aliases).
     * @return An update builder that corresponds to the SQL statement.
     *
     * @apiNote this method should not exist, instead the metadata itself should hold the synonym information.
     * TODO: remove once (TODO ([POST] Synonym support in Relational Metadata) is implemented.
     */
    @Nonnull
    UpdateStatement.Builder updateStatementBuilder(@Nonnull String updateQuery, @Nonnull Map<String, List<String>> columnSynonyms);

    @Nonnull
    UpdateStatement.Builder updateStatementBuilder(@Nonnull ParseTreeInfo parseTree);

    /**
     * Generates an {@link UpdateStatement.Builder} from a given update parse tree using the provided map of column synonyms.
     *
     * @param parseTree The update query parse tree.
     * @param columnSynonyms A map of column synonyms (aliases).
     * @return An update builder that corresponds to the SQL statement.
     *
     * @apiNote this method should not exist, instead the metadata itself should hold the synonym information.
     * TODO: remove once (TODO ([POST] Synonym support in Relational Metadata) is implemented.
     */
    @Nonnull
    UpdateStatement.Builder updateStatementBuilder(@Nonnull ParseTreeInfo parseTree, @Nonnull Map<String, List<String>> columnSynonyms);
}
