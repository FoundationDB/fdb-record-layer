/*
 * Key.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Namespace for classes related to {@link KeyExpression} construction and evaluation.
 */
@API(API.Status.STABLE)
public class Key {

    /**
     * Holder class for the static methods for creating Key Expressions.
     */
    public static class Expressions {
        /** The name of the key field in Protobuf {@code map} messages. */
        @Nonnull
        public static final String MAP_KEY_FIELD = "key";
        /** The name of the value field in Protobuf {@code map} messages. */
        @Nonnull
        public static final String MAP_VALUE_FIELD = "value";

        private Expressions() {
        }

        /**
         * Create an expression of a single scalar (i.e. not repeated) field. This sets the {@link Evaluated.NullStandin}
         * to {@link Evaluated.NullStandin#NULL}. See {@link #field(String, KeyExpression.FanType, com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin)}
         * for more details.
         *
         * @param name the name of the field to evaluate
         * @return a new Field expression, which can further be nested if the associated field is of the Message type.
         * @see #field(String, KeyExpression.FanType, com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin)
         */
        @Nonnull
        public static FieldKeyExpression field(@Nonnull String name) {
            return field(name, KeyExpression.FanType.None);
        }

        /**
         * Creates an expression of a field. This field can be repeated or not, depending on the fanType.
         * If fanType is Concatenate or None, this expression will only create 1 index entry. If it is FanOut it will
         * create 1 index entry per value of the field. This sets the {@link Evaluated.NullStandin} to
         * {@link Evaluated.NullStandin#NULL}. See {@link #field(String, KeyExpression.FanType, com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin)}
         * for more details.
         *
         * @param name the name of the field to evaluate
         * @param fanType the way that repeated fields should be handled
         * @return a new Field expression which can further be nested if the associated field is of the Message type.
         * @see #field(String, KeyExpression.FanType, com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin)
         */
        @Nonnull
        public static FieldKeyExpression field(@Nonnull String name, @Nonnull KeyExpression.FanType fanType) {
            return field(name, fanType, Evaluated.NullStandin.NULL);
        }

        /**
         * Creates an expression of a field. This field can be repeated or not, depending on the fanType.
         * If fanType is Concatenate or None, this expression will only create 1 index entry. If it is FanOut it will
         * create 1 index entry per value of the field.
         *
         * <p>
         * This also takes a {@link Evaluated.NullStandin} value to describe the field's behavior when it encounters an
         * unset field. If the value is {@link Evaluated.NullStandin#NOT_NULL NOT_NULL}, it will use the field's default
         * value instead of {@code null}. If the value is {@link Evaluated.NullStandin#NULL NULL}, it will return the
         * value {@code null} and disable any uniqueness checks (if the index is a {@linkplain IndexOptions#UNIQUE_OPTION unique}
         * index). If the value is {@link Evaluated.NullStandin#NULL_UNIQUE NULL_UNIQUE}, it will return {@code null}
         * and not disable any uniqueness checks. (In other words, it will treat {@code null} just like any other value.)
         * Note that for most use cases, the same value should be used for the {@code nullStandIn} every time the field
         * is referenced by any key expression or different indexes may have different ideas as to, for example,
         * whether a field can be {@code null} or not. For this reason, there are plans to move this expression from
         * the field to meta-data on the {@link RecordType}. See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/677">Issue #677</a>
         * for more details.
         * </p>
         *
         * @param name the name of the field to evaluate
         * @param fanType the way that repeated fields should be handled
         * @param nullStandin value to use in place of missing values,
         * determining whether it will contribute to unique indexes
         * @return a new Field expression which can further be nested if the associated field is of the Message type.
         */
        @Nonnull
        public static FieldKeyExpression field(@Nonnull String name, @Nonnull KeyExpression.FanType fanType, @Nonnull Evaluated.NullStandin nullStandin) {
            return new FieldKeyExpression(name, fanType, nullStandin);
        }

        /**
         * Concatenate multiple expressions together. This multiplies the index entry count of its children.
         * For example if the first expression generates two entries [a, b], and the second generates 3: [1, 2, 3], then
         * the concatenation will generate 6 index entries: [[a,1], [a,2], [a,3], [b,1], [b,2], [b,3]]. No context is
         * shared by this expression between it's children.
         * This flattens children, so concat(concat(field), concat(field2)) == concat(field1, field2).
         * @param first the first child expression to use in the index
         * @param second the second child expression to use in the index
         * @param rest this supports any number children (at least 2), this is the rest of them
         * @return a new expression which evaluates each child and returns the cross product
         */
        @Nonnull
        public static ThenKeyExpression concat(@Nonnull KeyExpression first, @Nonnull KeyExpression second,
                                               @Nonnull KeyExpression... rest) {
            return new ThenKeyExpression(first, second, rest);
        }

        /**
         * Concatenate multiple expressions together. This multiplies the index entry count of its children.
         * For example if the first expression generates two entries [a, b], and the second generates 3: [1, 2, 3], then
         * the concatenation will generate 6 index entries: [[a,1], [a,2], [a,3], [b,1], [b,2], [b,3]]. No context is
         * shared by this expression between it's children.
         * This flattens children, so concat(concat(field), concat(field2)) == concat(field1, field2).
         * @param children child expressions
         * @return a new expression which evaluates each child and returns the cross product
         */
        @Nonnull
        public static ThenKeyExpression concat(@Nonnull List<KeyExpression> children) {
            return new ThenKeyExpression(children);
        }

        /**
         * Shortuct for `concat(field(first), field(second), field(rest[0]), ...)`
         * @param first the name of the first field to use in the index
         * @param second the name of the second field to use in the index
         * @param fields this supports any number fields (at least 2), this is the rest of them
         * @return a new expression which evaluates each field returns them all in a single key
         */
        @Nonnull
        public static ThenKeyExpression concatenateFields(@Nonnull String first, @Nonnull String second,
                                                          @Nonnull String... fields) {
            KeyExpression[] rest = new KeyExpression[fields.length];
            for (int i = 0; i < fields.length; i++) {
                rest[i] = field(fields[i]);
            }
            return new ThenKeyExpression(field(first), field(second), rest);
        }

        /**
         * Concatenate multiple fields together.
         * @param fields names of the fields
         * @return a new expression which evaluates each field and returns them all in a single key
         */
        @Nonnull
        public static ThenKeyExpression concatenateFields(@Nonnull List<String> fields) {
            List<KeyExpression> exprs = new ArrayList<>(fields.size());
            for (String field : fields) {
                exprs.add(field(field));
            }
            return new ThenKeyExpression(exprs);
        }

        /**
         * Combine multiple expressions into a list.
         * @param children child expressions
         * @return a new expression which evaluates each child and returns the cross product as a list
         */
        @Nonnull
        public static ListKeyExpression list(@Nonnull KeyExpression... children) {
            return new ListKeyExpression(Arrays.asList(children));
        }

        /**
         * Combine multiple expressions into a list.
         * @param children child expressions
         * @return a new expression which evaluates each child and returns the cross product as a list
         */
        @Nonnull
        public static ListKeyExpression list(@Nonnull List<KeyExpression> children) {
            return new ListKeyExpression(children);
        }

        /**
         * Index key and value from a {@code map}.
         *
         * A map can be an actual Protobuf 3 {@code map} field or, 
         * in Protobuf 2, a {@code repeated} message field with fields named {@code key} and {@code value}.
         * @param name name of the map field
         * @return a new expression which evaluates to the key and value pairs of the given {@code map} field.
         */
        @Nonnull
        public static NestingKeyExpression mapKeyValues(@Nonnull String name) {
            return field(name, KeyExpression.FanType.FanOut).nest(concatenateFields(MAP_KEY_FIELD, MAP_VALUE_FIELD));
        }

        /**
         * Index key from a {@code map}.
         *
         * A map can be an actual Protobuf 3 {@code map} field or, 
         * in Protobuf 2, a {@code repeated} message field with fields named {@code key} and {@code value}.
         * @param name name of the map field
         * @return a new expression which evaluates to the values of the given {@code map} field.
         */
        @Nonnull
        public static NestingKeyExpression mapKeys(@Nonnull String name) {
            return field(name, KeyExpression.FanType.FanOut).nest(field(MAP_KEY_FIELD));
        }

        /**
         * Index value from a {@code map}.
         *
         * A map can be an actual Protobuf 3 {@code map} field or, 
         * in Protobuf 2, a {@code repeated} message field with fields named {@code key} and {@code value}.
         * @param name name of the map field
         * @return a new expression which evaluates to the values of the given {@code map} field.
         */
        @Nonnull
        public static NestingKeyExpression mapValues(@Nonnull String name) {
            return field(name, KeyExpression.FanType.FanOut).nest(field(MAP_VALUE_FIELD));
        }

        /**
         * Index value and key from a {@code map}.
         *
         * Like {@link #mapKeyValues}, but with the key and value field order reversed. This allows queries
         * such as equality on the value and inequality on the key or equality on the value ordered by the key.
         *
         * A map can be an actual Protobuf 3 {@code map} field or, 
         * in Protobuf 2, a {@code repeated} message field with fields named {@code key} and {@code value}.
         * @param name name of the map field
         * @return a new expression which evaluates to the value and key pairs of the given {@code map} field.
         */
        @Nonnull
        public static NestingKeyExpression mapValueKeys(@Nonnull String name) {
            return field(name, KeyExpression.FanType.FanOut).nest(concatenateFields(MAP_VALUE_FIELD, MAP_KEY_FIELD));
        }

        /**
         * Create a function call with arguments that will result in a key expression.
         * @param name the name of the function
         * @param arguments the arguments to the function
         * @return a new key expression that evaluates by calling the named function
         */
        @Nonnull
        public static FunctionKeyExpression function(@Nonnull String name, @Nonnull KeyExpression arguments) {
            return FunctionKeyExpression.create(name, arguments);
        }

        /**
         * Create a function call with arguments that will result in a key expression.
         * @param name the name of the function
         * @return a new key expression that evaluates by calling the named function
         */
        @Nonnull
        public static FunctionKeyExpression function(@Nonnull String name) {
            return FunctionKeyExpression.create(name, EmptyKeyExpression.EMPTY);
        }

        /**
         * Create a static scalar value of type <code>T</code>. The typical use case for this would be to
         * pass arguments to a function call (see {@link FunctionKeyExpression}).
         * @param value the {@code T} value
         * @param <T> the type of the value
         * @return a new key expression that evaluates to the given value
         */
        @Nonnull
        public static <T> LiteralKeyExpression<T> value(@Nullable T value) {
            return new LiteralKeyExpression<T>(value);
        }

        /**
         * A <code>keyWithValue</code> expression is a top level expression that takes as input an expression that
         * produces more than one column, and indicates that all columns before the specified <code>splitPoint</code>
         * should be used in the key of an index and all of the values starting at the <code>splitPoint</code>
         * are to be used as the value of the index.
         * @param child the key expression to split
         * @param splitPoint the position in the key result to split
         * keys before this position are part of the key and keys after this position are part of the value
         * @return an expression splitting the key and value
         */
        @Nonnull
        public static KeyWithValueExpression keyWithValue(@Nonnull KeyExpression child, int splitPoint) {
            return new KeyWithValueExpression(child, splitPoint);
        }

        /**
         * The empty key expression.
         * @return the empty key expression
         * @see EmptyKeyExpression
         */
        @Nonnull
        public static EmptyKeyExpression empty() {
            return EmptyKeyExpression.EMPTY;
        }

        /**
         * The version key expression, which indicates that a versionstamp should be contained within the key.
         * @return the version key expression
         * @see VersionKeyExpression
         */
        @Nonnull
        public static VersionKeyExpression version() {
            return VersionKeyExpression.VERSION;
        }

        /**
         * The record type key expression, which indicates that a unique record type identifier should
         * be contained within the key.
         * @return the record type key expression
         * @see RecordTypeKeyExpression
         */
        @Nonnull
        public static RecordTypeKeyExpression recordType() {
            return RecordTypeKeyExpression.RECORD_TYPE_KEY;
        }

        /**
         * Convert a field descriptor into a expression. Assumes FanOut for repeated keys.
         * @param fieldDescriptor a field in a record type.
         * @return a new {@code Field} to get the value of the given field descriptor.
         */
        @Nonnull
        public static FieldKeyExpression fromDescriptor(@Nonnull Descriptors.FieldDescriptor fieldDescriptor) {
            final FieldKeyExpression field = fieldDescriptor.isRepeated() ?
                                             Expressions.field(fieldDescriptor.getName(), KeyExpression.FanType.FanOut) :
                                             Expressions.field(fieldDescriptor.getName());
            field.validate(fieldDescriptor.getContainingType(), fieldDescriptor, false);
            return field;
        }

        /**
         * Determine whether the given key expression has a record type key prefix.
         * @param key key expression to check
         * @return {@code true} if the given key expression has a record type key prefix
         */
        public static boolean hasRecordTypePrefix(@Nonnull KeyExpression key) {
            if (key instanceof RecordTypeKeyExpression) {
                return true;
            } else if (key instanceof GroupingKeyExpression) {
                GroupingKeyExpression grouping = (GroupingKeyExpression)key;
                return grouping.getGroupingCount() > 0 && hasRecordTypePrefix(grouping.getWholeKey());
            } else if (key instanceof KeyWithValueExpression) {
                KeyWithValueExpression keyWithValue = (KeyWithValueExpression)key;
                return keyWithValue.getSplitPoint() > 0 && hasRecordTypePrefix(keyWithValue.getInnerKey());
            } else if (key instanceof ThenKeyExpression) {
                ThenKeyExpression then = (ThenKeyExpression)key;
                return !then.getChildren().isEmpty() && hasRecordTypePrefix(then.getChildren().get(0));
            } else if (key instanceof NestingKeyExpression) {
                return hasRecordTypePrefix(((NestingKeyExpression)key).getChild());
            } else {
                return false;
            }
        }
    }

    /**
     * Represents the list objects for a key in a secondary index or primary storage.
     * To convert this into a Tuple for storage, simply call `Tuple.from(evaluated.toList())`
     */
    public static class Evaluated {

        /**
         * Values used in index keys in place of missing fields.
         */
        public enum NullStandin {
            NULL(RecordMetaDataProto.Field.NullInterpretation.NOT_UNIQUE), // Missing field here skips uniqueness checks.
            NULL_UNIQUE(RecordMetaDataProto.Field.NullInterpretation.UNIQUE), // Missing field here like ordinary value, but null, for uniqueness.
            NOT_NULL(RecordMetaDataProto.Field.NullInterpretation.NOT_NULL); // Missing field has type's ordinary default value.

            private RecordMetaDataProto.Field.NullInterpretation proto;

            NullStandin(RecordMetaDataProto.Field.NullInterpretation nullInterpretation) {
                proto = nullInterpretation;
            }

            public RecordMetaDataProto.Field.NullInterpretation toProto() {
                return proto;
            }

            public static NullStandin valueOf(RecordMetaDataProto.Field.NullInterpretation nullInterpretation) throws KeyExpression.DeserializationException {
                switch (nullInterpretation) {
                    case NOT_UNIQUE:
                        return NULL;
                    case UNIQUE:
                        return NULL_UNIQUE;
                    case NOT_NULL:
                        return NOT_NULL;
                    default:
                        throw new KeyExpression.DeserializationException("Invalid null interpretation" + nullInterpretation);
                }
            }
        }

        /**
         * A key containing no elements.
         */
        public static final Evaluated EMPTY = new Evaluated(Collections.emptyList());

        /**
         * A key containing just the null element.
         */
        public static final Evaluated NULL = new Evaluated(Collections.singletonList(NullStandin.NULL));

        @Nonnull
        private List<Object> values;

        @Nullable
        private List<Object> tupleAppropriateList = null; // Lazily cached tuple appropriate values

        @Nullable
        private Tuple tuple = null;  // Lazily cached tuple

        /**
         * Creates a new key with just a single value.
         * @param object the lone value in the key
         * @return a new key for the one value
         */
        @Nonnull
        public static Evaluated scalar(@Nullable Object object) {
            return new Evaluated(Collections.singletonList(object));
        }

        /**
         * Fanout a list of scalars into a key for each value.
         * @param valuesToAdd the list of scalars
         * @return one key for each value in valuesToAdd
         */
        @Nonnull
        public static List<Evaluated> fan(@Nonnull List<Object> valuesToAdd) {
            List<Evaluated> newValues = new ArrayList<>(valuesToAdd.size());
            for (Object object : valuesToAdd) {
                newValues.add(Evaluated.scalar(object));
            }
            return newValues;
        }

        /**
         * Take a list of scalars and concatenate them in a single key. Note this is a key containing the list of values,
         * as opposed to a key with a single value that is a list.
         * @param values the values to combine together into a key
         * @return a new key with the given values
         */
        @Nonnull
        public static Evaluated concatenate(@Nonnull List<Object> values) {
            return new Evaluated(values);
        }

        /**
         * Primarily for tests, shorthand for creating a concatenation with a bunch of elements, see concatenate.
         * @param first the first value to be concatenated, avoids type conflicts with the other concatenate
         * @param rest the rest of the values
         * @return a new key consisting of the provided values
         */
        @Nonnull
        public static Evaluated concatenate(@Nullable Object first, @Nullable Object... rest) {
            final ArrayList<Object> values = new ArrayList<>(rest.length + 1);
            values.add(first);
            Collections.addAll(values, rest);
            return new Evaluated(values);
        }

        /**
         * Take a {@link Tuple} and create the corresponding
         * key from it.
         * @param tuple the tuple to convert
         * @return a new key based on the given tuple
         */
        @Nonnull
        public static Evaluated fromTuple(@Nonnull Tuple tuple) {
            return new Evaluated(tuple.getItems());
        }

        /**
         * Convert this key to the corresponding {@link Tuple} that can then be serialized
         * into the database.
         * @return this key converted into a {@link Tuple}
         */
        @Nonnull
        public Tuple toTuple() {
            if (tuple == null) {
                tuple = Tuple.fromList(toTupleAppropriateList());
            }
            return tuple;
        }

        Evaluated(@Nonnull List<Object> values) {
            this.values = values;
        }

        @Override
        @SpotBugsSuppressWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Evaluated evaluated = (Evaluated) o;

            return !(values != null ? !values.equals(evaluated.values) : evaluated.values != null);

        }

        @Override
        @SpotBugsSuppressWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
        public int hashCode() {
            return values != null ? values.hashCode() : 0;
        }

        @Override
        public String toString() {
            return values.toString();
        }

        @Nullable
        public Object getObject(int idx) {
            return TupleTypeUtil.toTupleAppropriateValue(values.get(idx));
        }

        @Nullable
        @SuppressWarnings("PMD.PreserveStackTrace")
        public <T> T getObject(int idx, Class<T> clazz) {
            final Object result = TupleTypeUtil.toTupleAppropriateValue(values.get(idx));
            try {
                return clazz.cast(result);
            } catch (ClassCastException e) {
                throw new KeyExpression.InvalidResultException("Invalid type in value").addLogInfo(
                        "index", idx,
                        LogMessageKeys.EXPECTED_TYPE, clazz.getName(),
                        LogMessageKeys.ACTUAL_TYPE, result.getClass().getName());
            }
        }

        @Nonnull
        private <T> T notNull(int idx, @Nullable T value) {
            if (value == null) {
                throw new KeyExpression.InvalidResultException("Unexpected null value")
                        .addLogInfo("index", idx);
            }
            return value;
        }

        public long getLong(int idx) {
            return notNull(idx, getObject(idx, Number.class)).longValue();
        }

        @Nullable
        public Long getNullableLong(int idx) {
            Number value = getObject(idx, Number.class);
            return (value == null ? null : (value instanceof Long) ? (Long) value : value.longValue());
        }

        public double getDouble(int idx) {
            return notNull(idx, getObject(idx, Number.class)).doubleValue();
        }

        @Nullable
        public Double getNullableDouble(int idx) {
            Number value = getObject(idx, Number.class);
            return (value == null ? null : (value instanceof Double) ? (Double) value : value.doubleValue());
        }

        public float getFloat(int idx) {
            return notNull(idx, getObject(idx, Number.class)).floatValue();
        }

        @Nullable
        public Float getNullableFloat(int idx) {
            Number value = getObject(idx, Number.class);
            return (value == null ? null : (value instanceof Float) ? (Float) value : value.floatValue());
        }

        @Nullable
        public String getString(int idx) {
            return getObject(idx, String.class);
        }

        /**
         * Creates a new key by appending another key to this one.
         * @param secondValue another key that will form the second part of the new key.
         * @return a new key consisting of the elements in this key, and then the elements of the second key
         */
        @Nonnull
        public Evaluated append(@Nonnull Evaluated secondValue) {
            final ArrayList<Object> combined = new ArrayList<>(values);
            combined.addAll(secondValue.values);
            return new Evaluated(combined);
        }

        /**
         * Converts to a list. Useful for creating tuples
         * @return the elements of this key value
         */
        @Nonnull
        public List<Object> toList() {
            return toTupleAppropriateList();
        }

        /**
         * Converts to a list. Useful for creating tuples. If possible the underlying objects will be converted to
         * types that Tuple can handle.
         * @return the elements of this key value
         */
        @Nonnull
        public List<Object> toTupleAppropriateList() {
            if (tupleAppropriateList == null) {
                tupleAppropriateList = TupleTypeUtil.toTupleAppropriateList(values);
            }
            return tupleAppropriateList;
        }

        /**
         * Check for NULL value that inhibits unique indexes.
         * @return <code>true</code> if this has a null value
         */
        public boolean containsNonUniqueNull() {
            return values.indexOf(NullStandin.NULL) >= 0;
        }

        public List<Object> values() {
            return values;
        }

        public int size() {
            return values.size();
        }

        public Evaluated subKey(int start, int end) {
            return new Evaluated(values.subList(start, end));
        }
    }

    private Key() {
    }
}
