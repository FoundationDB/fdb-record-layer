/*
 * IndexEntrySource.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpression;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A source of abstract index entries, each of which references a single record. An {@code IndexEntrySource} is an
 * abstraction of the disparate sources of index entries. Presently, an {@code IndexEntrySource} is either an index
 * or the range of primary keys, but in general there could be other index entry sources.
 *
 * <p>
 * This abstraction is quite useful for the various rules in {@link com.apple.foundationdb.record.query.plan.temp.rules}
 * since most rules can consider different types of scans that produces records with a uniform interface.
 * </p>
 */
public class IndexEntrySource {
    @Nullable
    private final Index index; // null indicates primary key
    @Nonnull
    private final ViewExpressionComparisons root;

    private IndexEntrySource(@Nullable Index index, @Nonnull ViewExpressionComparisons root) {
        this.index = index;
        this.root = root;
    }

    @Nonnull
    public ViewExpressionComparisons getEmptyComparisons() {
        return root;
    }

    @Nullable
    public String getIndexName() {
        return index == null ? null : index.getName();
    }

    public boolean isIndexScan() {
        return index != null;
    }

    @Nonnull
    public static IndexEntrySource fromIndex(@Nonnull Collection<RecordType> recordTypes, @Nonnull Index index) {
        return new IndexEntrySource(index, new ViewExpressionComparisons(
                ViewExpression.fromIndexDefinition(index.getType(),
                        recordTypes.stream().map(RecordType::getName).collect(Collectors.toSet()),
                        index.getRootExpression())));
    }

    @Nonnull
    public static IndexEntrySource fromIndexWithTypeStrings(@Nonnull Collection<String> recordTypes, @Nonnull Index index) {
        return new IndexEntrySource(index, new ViewExpressionComparisons(
                ViewExpression.fromIndexDefinition(index.getType(), recordTypes, index.getRootExpression())));
    }

    @Nonnull
    public static IndexEntrySource fromCommonPrimaryKey(@Nonnull Collection<String> recordTypes, @Nonnull KeyExpression commonPrimaryKey) {
        return new IndexEntrySource(null, new ViewExpressionComparisons(
                ViewExpression.fromIndexDefinition(IndexTypes.VALUE, recordTypes, commonPrimaryKey)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexEntrySource that = (IndexEntrySource)o;
        return Objects.equals(index, that.index) &&
               Objects.equals(root, that.root);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, root);
    }

    @Override
    public String toString() {
        if (index == null) {
            return "PRIMARY_KEY";
        } else {
            return index.getName();
        }
    }
}
