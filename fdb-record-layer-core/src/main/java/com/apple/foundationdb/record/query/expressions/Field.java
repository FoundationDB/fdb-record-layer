/*
 * Field.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * Class that provides context for asserting about a field value.
 * {@link #oneOfThem()} and {@link #matches(QueryComponent)} allow you to create other components, while
 * the remaining functions allow you to match on the value of the associated field directly.
 */
@API(API.Status.UNSTABLE)
public class Field {
    @Nonnull
    private final String fieldName;

    public Field(@Nonnull String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * How an empty / unset repeated field should be handled.
     */
    public enum OneOfThemEmptyMode {
        /** An empty repeated field causes {@code oneOfThem} predicates to return UNKNOWN, like a scalar NULL value. */
        EMPTY_UNKNOWN,
        /** An empty repeated field is treated like any other list, just with no elements, so none can match. */
        EMPTY_NO_MATCHES
        // TODO: A mode that depends on the nullability (versus field default value) of the record type's field in the descriptor / meta-data.
    }

    /**
     * If the associated field is a submessage, this allows you to match against the fields within that submessage.
     * The child is evaluated and validated in the context of this field, not the record containing this field.
     * @param child a component asserting about the content of the submessage in this field
     * @return a new component ready for evaluation
     */
    @Nonnull
    public QueryComponent matches(@Nonnull QueryComponent child) {
        return new NestedField(fieldName, child);
    }

    /**
     * If the associated field is a repeated one, this allows you to match against one of the repeated values.
     * The record will be returned if any one of the values returns {@code true}. The same record may be returned more than once.
     * If the repeated field is empty, the match result will be UNKNOWN.
     * @return an OneOfThem that can have further assertions called on it about the value of the given field
     */
    @Nonnull
    public OneOfThem oneOfThem() {
        return new OneOfThem(fieldName);
    }

    /**
     * If the associated field is a repeated one, this allows you to match against one of the repeated values.
     * The record will be returned if any one of the values returns {@code true}. The same record may be returned more than once.
     * @param emptyMode whether an empty repeated field should cause an UNKNOWN result instead of failing to match any (and so returning FALSE)
     * @return an OneOfThem that can have further assertions called on it about the value of the given field
     */
    @Nonnull
    public OneOfThem oneOfThem(OneOfThemEmptyMode emptyMode) {
        return new OneOfThem(fieldName, emptyMode);
    }

    /**
     * Checks if the field has a value equal to the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent equalsValue(@Nonnull Object comparand) {
        return fieldWithComparison(fieldName, Comparisons.Type.EQUALS, comparand);
    }

    /**
     * Checks if the field has a value not equal to the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notEquals(@Nonnull Object comparand) {
        return fieldWithComparison(fieldName, Comparisons.Type.NOT_EQUALS, comparand);
    }

    /**
     * Checks if the field has a value greater than the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThan(@Nonnull Object comparand) {
        return new FieldWithComparison(fieldName,
                new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, comparand));
    }

    /**
     * Checks if the field has a value greater than or equal to the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThanOrEquals(@Nonnull Object comparand) {
        return new FieldWithComparison(fieldName,
                new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand));
    }

    /**
     * Checks if the field has a value less than the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThan(@Nonnull Object comparand) {
        return new FieldWithComparison(fieldName,
                new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, comparand));
    }

    /**
     * Checks if the field has a value less than or equal to the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThanOrEquals(@Nonnull Object comparand) {
        return new FieldWithComparison(fieldName,
                new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand));
    }

    /**
     * Checks if the field starts with the given string.
     * Requires that the field contains a string.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent startsWith(@Nonnull String comparand) {
        return fieldWithComparison(fieldName, Comparisons.Type.STARTS_WITH, comparand);
    }

    /**
     * Checks if the field starts with the given byte string.
     * Requires that the field contains a byte string.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent startsWith(@Nonnull ByteString comparand) {
        return fieldWithComparison(fieldName, Comparisons.Type.STARTS_WITH, comparand);
    }

    /**
     * Returns true if the field has not been set and uses the default value.
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent isNull() {
        return new FieldWithComparison(fieldName, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
    }

    /**
     * Returns true if the field does not use the default value.
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notNull() {
        return new FieldWithComparison(fieldName, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
    }

    /**
     * Returns true if the repeated field does not have any occurrences.
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent isEmpty() {
        return new EmptyComparison(fieldName, true);
    }

    /**
     * Returns true if the repeated field has occurrences.
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notEmpty() {
        return new EmptyComparison(fieldName, false);
    }

    /**
     * Query where the value of this field is in the given list.
     * @param comparand a list of elements
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent in(@Nonnull List<?> comparand) {
        return new FieldWithComparison(fieldName, new Comparisons.ListComparison(Comparisons.Type.IN, comparand));
    }

    /**
     * Query where the value of this field is in the list that is bound to the given param.
     * @param param a param that will be bound to a list in the execution context
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent in(@Nonnull String param) {
        return new FieldWithComparison(fieldName, new Comparisons.ParameterComparison(Comparisons.Type.IN, param));
    }

    @Nonnull
    private QueryComponent fieldWithComparison(@Nonnull String fieldName, @Nonnull Comparisons.Type type,
                                               @Nonnull Object comparand) {
        if (comparand instanceof List) {
            @SuppressWarnings("rawtypes")
            List list = (List) comparand;
            if (!list.isEmpty()) {
                return new FieldWithComparison(fieldName,
                        new Comparisons.ListComparison(type, list));
            } else {
                throw new RecordCoreException("List comparand must have at least one element");
            }
        } else {
            return new FieldWithComparison(fieldName,
                    new Comparisons.SimpleComparison(type, comparand));
        }
    }

    /**
     * Query where the value of this field matches some property when performing a full-text
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
        return new FieldText(fieldName);
    }

    /**
     * Query where the value of this field matches some property when performing a full-text
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
        return new FieldText(fieldName, tokenizerName);
    }

    /**
     * Query where the value of this field matches some property when performing a full-text
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
        return new FieldText(fieldName, tokenizerName, defaultTokenizerName);
    }

    /**
     * Checks if the field has a value equal to the given parameter.
     * Evaluates to null if the field does not have a value.
     * @param param the name of the parameter
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent equalsParameter(@Nonnull String param) {
        return new FieldWithComparison(fieldName, new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, param));
    }

    /**
     * Match {@code map} field entries.
     *
     * <pre><code>
     * Query.field("map_field").mapMatches(k -&gt; k.equals("akey"), v -&gt; v.greaterThan(10))
     * </code></pre>
     * @param keyMatcher a function to apply to the key {@link Field} or {@code null} not to restrict the key
     * @param valueMatcher a function to apply to the value {@link Field} or {@code null} not to restrict the value
     * @return a new component that will return the record if the map field has an entry matching the key and value criteria
     */
    @Nonnull
    public QueryComponent mapMatches(@Nullable Function<Field, QueryComponent> keyMatcher,
                                     @Nullable Function<Field, QueryComponent> valueMatcher) {
        final QueryComponent component;
        if (keyMatcher != null) {
            if (valueMatcher != null) {
                component = Query.and(keyMatcher.apply(new Field(Key.Expressions.MAP_KEY_FIELD)),
                                      valueMatcher.apply(new Field(Key.Expressions.MAP_VALUE_FIELD)));
            } else {
                component = keyMatcher.apply(new Field(Key.Expressions.MAP_KEY_FIELD));
            }
        } else if (valueMatcher != null) {
            component = valueMatcher.apply(new Field(Key.Expressions.MAP_VALUE_FIELD));
        } else {
            throw new Query.InvalidExpressionException("must match the key or the value or both");
        }
        return oneOfThem().matches(component);
    }
}
