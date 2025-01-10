/*
 * OneOfThem.java
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
import javax.annotation.Nullable;
import java.util.List;

/**
 * Class for matching any value of a repeated field.
 * The components created by this class will evaluate to true if any of the values of the associated field evaluate
 * to true. Unless specified otherwise, none of the components require that the record be returned only once.
 * The child components only see one of the values when evaluating.
 */
@API(API.Status.UNSTABLE)
public class OneOfThem {
    @Nonnull
    private final String fieldName;
    private final Field.OneOfThemEmptyMode emptyMode;

    public OneOfThem(@Nonnull String fieldName) {
        this(fieldName, Field.OneOfThemEmptyMode.EMPTY_UNKNOWN);
    }

    public OneOfThem(@Nonnull String fieldName, Field.OneOfThemEmptyMode emptyMode) {
        this.fieldName = fieldName;
        this.emptyMode = emptyMode;
    }

    /**
     * If the associated field is a submessage, this allows you to match against the fields within that submessage.
     * The child is evaluated and validated in the context of this field and not the record containing this field.
     * @param child a component asserting about the content of the submessage in this field
     * @return a new component ready for evaluation
     */
    @Nonnull
    public QueryComponent matches(@Nonnull QueryComponent child) {
        return new OneOfThemWithComponent(fieldName, emptyMode, child);
    }

    /**
     * Checks if one of the values in this repeated field is equal to the given comparand.
     * Evaluates to null if there are no values for the field
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent equalsValue(@Nonnull Object comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is not equal to the given comparand.
     * Evaluates to null if there are no values for the field
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notEquals(@Nonnull Object comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is greater than the given comparand.
     * Evaluates to null if there are no values for the field
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThan(@Nonnull Object comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is greater than or equal to the given comparand.
     * Evaluates to null if there are no values for the field
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThanOrEquals(@Nonnull Object comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is less than the given comparand.
     * Evaluates to null if there are no values for the field
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThan(@Nonnull Object comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is less than or equal to the given comparand.
     * Evaluates to null if there are no values for the field
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThanOrEquals(@Nonnull Object comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand));
    }

    /**
     * Checks if one of the values in this repeated field starts with the given string.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent startsWith(@Nonnull String comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.SimpleComparison(Comparisons.Type.STARTS_WITH, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is in the given list.
     * @param comparand a list of elements
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent in(@Nonnull List<?> comparand) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.ListComparison(Comparisons.Type.IN, comparand));
    }

    /**
     * Checks if one of the values in this repeated field is in the list that is bound to the given param.
     * @param param a param that will be bound to a list in the execution context
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent in(@Nonnull String param) {
        return new OneOfThemWithComparison(fieldName, emptyMode,
                new Comparisons.ParameterComparison(Comparisons.Type.IN, param));
    }

    /**
     * Checks if one of the values in this repeated field matches some property when performing a full-text
     * search. It does not specify any tokenizer information, so it is assumed that any tokenizer
     * is acceptable for parsing query string (if not already tokenized) or to parse the record's text
     * or to match against an index. If an index is used during query execution, then it is guaranteed that
     * the same tokenizer that was used to tokenize the index is used to tokenize the query string (if not
     * already tokenized).
     *
     * @return an intermediate object to use to select the appropriate predicate
     */
    @Nonnull
    public Text text() {
        return new OneOfThemText(fieldName, emptyMode);
    }

    /**
     * Checks if one of the values in this repeated field matches some property when performing a full-text
     * search. If <code>tokenizerName</code> is not <code>null</code>, then this will use that tokenizer
     * to tokenize the query string (if not already tokenized) and to parse the record's text. If
     * an index is used, then it is guaranteed that the index uses this same tokenizer. If
     * <code>tokenizerName</code> is <code>null</code>, then this behaves the same as {@link #text()}.
     *
     * @param tokenizerName the name of the tokenizer to use to tokenize the record and (if necessary) the query string
     * @return an intermediate object to use to select the appropriate predicate
     */
    @Nonnull
    public Text text(@Nullable String tokenizerName) {
        return new OneOfThemText(fieldName, emptyMode, tokenizerName);
    }

    /**
     * Checks if one of the values in this repeated field matches some property when performing a full-text
     * search. If <code>tokenizerName</code> is not <code>null</code>, then this will use that tokenizer
     * to tokenize the query string (if not already tokenized) and to parse the record's text. If
     * an index is used, then it is guaranteed that the index uses this same tokenizer. If
     * <code>tokenizerName</code> is <code>null</code>, then this behaves the same as {@link #text()}
     * except that if <code>defaultTokenizerName</code> is not <code>null</code>, then it will use
     * that tokenizer to tokenize the document and query string (if no index is available) instead
     * of the normal default tokenizer, {@link com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer DefaultTextTokenizer}.
     *
     * @param tokenizerName the name of the tokenizer to use to tokenize the record and (if necessary) the query string
     * @param defaultTokenizerName the name of the tokenizer to use if <code>tokenizerName</code> is <code>null</code>
     *                             and no index can be found to satisfy the query
     * @return an intermediate object to use to select the appropriate predicate
     */
    @Nonnull
    public Text text(@Nullable String tokenizerName, @Nullable String defaultTokenizerName) {
        return new OneOfThemText(fieldName,  emptyMode, tokenizerName, defaultTokenizerName);
    }
}
