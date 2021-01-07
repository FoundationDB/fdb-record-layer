/*
 * Text.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * Predicates that can be applied to a field that has been indexed with a full-text index. These
 * allow for 3 options
 *
 * 1.  Supplying a Lucene Query Syntax Query
 * 2.  Supplying a Lucene Query Syntax Query and exp
 * 3.  Autocomplete
 *
 * <p>
 * This should be created by calling the {@link Field#text() text()} method on a query
 * {@link Field} or {@link OneOfThem} instance. For example, one might call: <code>Query.field("text").text()</code>
 * to create a predicate on the <code>text</code> field's contents.
 * </p>
 *
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneText {

    @Nonnull
    public QueryComponent luceneQuery(@Nonnull String query) {
        return new LuceneQueryComponent(query);
    }

    @Nonnull
    public QueryComponent luceneQueryWithHighlight(@Nonnull String query) {
        return new LuceneQueryComponent(query);
    }

    @Nonnull
    public QueryComponent autoComplete(@Nonnull String fieldName, @Nonnull String query) {
        return new LuceneQueryComponent(query);
    }

}
