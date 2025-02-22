/*
 * KeyExpression.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.QueryHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.ExpansionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Interface for expressions that evaluate to keys.
 * While Java will let you extend this class, you probably shouldn't because the planner does lots of instanceof
 * calls to figure out what query to use. If you're ok with always just doing a scan of all records, and evaluating
 * the expression on that, I guess that's ok.
 *
 * When implementing a new key expression, care should be taken to implement at least one (and possibly both) of
 * the interfaces Key.AtomExpression and Key.ExpressionWithChildren.
 */
@API(API.Status.UNSTABLE)
public interface KeyExpression extends PlanHashable, QueryHashable {
    /**
     * Evaluate against a given record producing a list of evaluated keys. These are extracted from the
     * fields within the record according to the rules of each implementing class.
     *
     * Implementations should override {@link #evaluateMessage} instead of this one, even if they do not deal with
     * Protobuf messages, so that they interact properly with expressions that do.
     * @param <M> the type of record
     * @param record the record
     * @return the list of evaluated keys for the given record
     * @throws InvalidResultException if any returned result has some number of columns other
     *         than the return value of {@link #getColumnSize()}
     */
    @Nonnull
    default <M extends Message> List<Key.Evaluated> evaluate(@Nullable FDBRecord<M> record) {
        return evaluateMessage(record, record == null ? null : record.getRecord());
    }

    /**
     * Evaluate this expression with the expectation of getting exactly one result.
     * @param <M> the type of record
     * @param record the record
     * @return the evaluated keys for the given record
     */
    @Nonnull
    default <M extends Message> Key.Evaluated evaluateSingleton(@Nullable FDBRecord<M> record) {
        final List<Key.Evaluated> keys = evaluate(record);
        if (keys.size() != 1) {
            throw new RecordCoreException("Should evaluate to single key only");
        }
        return keys.get(0);
    }

    /**
     * Evaluate against a given record producing a list of evaluated keys. These are extracted from the
     * fields within the record according to the rules of each implementing class.
     *
     * Implementations should override {@link #evaluateMessage} instead of this one, even if they do not deal with
     * Protobuf messages, so that they interact properly with expressions that do.
     * @param <M> the type of record
     * @param record the record
     * @param message the Protobuf message to evaluate against
     * @return the list of evaluated keys for the given record
     * @throws InvalidResultException if any returned result has some number of columns other
     *         than the return value of {@link #getColumnSize()}
     */
    @Nonnull
    default <M extends Message> Key.Evaluated evaluateMessageSingleton(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final List<Key.Evaluated> keys = evaluateMessage(record, message);
        if (keys.size() != 1) {
            throw new RecordCoreException("Should evaluate to single key only");
        }
        return keys.get(0);
    }

    /**
     * Evaluate this expression against a record or a Protobuf message.
     *
     * The message might be the Protobuf form of a record or a piece of that record.
     * If the key expression is meaningful against a subrecord, it should evaluate against the message.
     * Otherwise, it should evaluate against the record and ignore what part of that record is being considered.
     *
     * There should not be any reason to call this method outside of the implementation of another {@code evaluateMessage}.
     * Under ordinary circumstances, if {@code record} is {@code null}, then {@code message} will be {@code null}.
     * Otherwise, {@code message} will be {@code record.getRecord()} or some submessage of that, possibly {@code null} if
     * the corresponding field is missing.
     * @see #evaluate
     * @param <M> the type of record
     * @param record the record
     * @param message the Protobuf message to evaluate against
     * @return the evaluated keys for the given record
     */
    @Nonnull
    <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message);

    /**
     * This states whether the given expression type is capable of evaluating more to more than
     * one value when applied to a single record. In practice, this can happen if this expression
     * is evaluated on a repeated field with {@link FanType#FanOut FanType.FanOut} set (either directly
     * or indirectly).
     *
     * @return <code>true</code> if this expression can evaluate to multiple values and <code>false</code>
     * otherwise
     */
    boolean createsDuplicates();

    /**
     * Validate this expression against a given record type descriptor.
     * @param descriptor the descriptor for the record type or submessage
     * @return a list of fields that this key applies to
     * @throws InvalidExpressionException
     * if the expression is not valid for the given descriptor
     */
    List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor);

    /**
     * Returns the number of items in each KeyValue that will be returned. For key expressions that support
     * {@link FanType#Concatenate}, this will the count of non-nested lists, i.e. this will be value of
     * evaluate(r).get(i).toList().size() for any i or r.
     *
     * @return the number of elements that will be produced for every key
     */
    int getColumnSize();

    /**
     * How should repeated fields be handled.
     * These names don't technically meet our naming convention but changing them is a lot of work because of all of
     * the string constants.
     */
    @SuppressWarnings({"squid:S00115", "PMD.FieldNamingConventions"})
    enum FanType {
        /**
         * Convert a repeated field into a single list.
         * This does not cause the number of index values to increase.
         */
        Concatenate(RecordKeyExpressionProto.Field.FanType.CONCATENATE),
        /**
         * Create an index value for each value of the field.
         */
        FanOut(RecordKeyExpressionProto.Field.FanType.FAN_OUT),
        /**
         * Nothing, only allowed with scalar fields.
         */
        None(RecordKeyExpressionProto.Field.FanType.SCALAR);

        private RecordKeyExpressionProto.Field.FanType proto;

        FanType(RecordKeyExpressionProto.Field.FanType fanType) {

            proto = fanType;
        }

        RecordKeyExpressionProto.Field.FanType toProto() {
            return proto;
        }

        public static FanType valueOf(RecordKeyExpressionProto.Field.FanType fanType) throws DeserializationException {
            switch (fanType) {
                case SCALAR:
                    return None;
                case FAN_OUT:
                    return FanOut;
                case CONCATENATE:
                    return Concatenate;
                default:
                    throw new DeserializationException("Invalid fan type " + fanType);
            }
        }
    }

    @Nonnull
    Message toProto() throws SerializationException;

    @Nonnull
    RecordKeyExpressionProto.KeyExpression toKeyExpression();

    /**
     * Get key in normalized form for comparing field-by-field.
     * Does not take account of fan-type, so only valid
     * when this has already been taken care of, such as individual index entries.
     * @return a list of key expressions in order
     */
    @Nonnull
    default List<KeyExpression> normalizeKeyForPositions() {
        return Collections.singletonList(this);
    }

    /**
     * Method that indicates to the caller if this key expression can be normalized and re-composed in a way
     * that yields an expression equivalent to the original.
     *
     * @return {@code true} if the key expression has a lossless normalization, {@code false} otherwise
     */
    @API(API.Status.INTERNAL)
    default boolean hasLosslessNormalization() {
        return true;
    }

    /**
     * Method to determine if data in an index represented by this key expression needs to be copied from an
     * index entry to a partial record. For instance all {@link LiteralKeyExpression}s are constant and not dependent on
     * an input record. Similarly, when scanning a single type index, the record type of each record is known, however,
     * it is not part of the base record, or by extension the message contained therein.
     * This information is needed to allow key expressions to partake in covering index scans that are not
     * provided by the underlying index entries, but can easily be computed on-the-fly.
     * @return {@code true} if the key expression needs to be copied from index entry to partial record.
     */
    @API(API.Status.INTERNAL)
    default boolean needsCopyingToPartialRecord() {
        return true;
    }

    /**
     * Expand this key expression into a data flow graph. The returned graph represents an adequate representation
     * of the key expression as composition of relational expressions and operators
     * ({@link RelationalExpression}s). Note that implementors should
     * defer to the visitation methods in the supplied visitor rather than encoding actual logic in an overriding
     * method.
     * @param <S> a type that represents the state that {@code visitor} uses
     * @param <R> a type that represents the result the {@code visitor} returns
     * @param visitor a {@link ExpansionVisitor} that is created by the caller from a data structure that reflects the
     *        specific semantics of the key expression. Normally this visitor is directly created based on an index.
     * @return a new {@link GraphExpansion} representing the query graph equivalent of this key expression using the
     *         supplied visitor
     * @see com.apple.foundationdb.record.query.expressions.QueryComponent#expand
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull KeyExpressionVisitor<S, R> visitor);

    /**
     * Return the key fields for an expression.
     * @param rootExpression a key expression
     * @return the parts of the key expression that are in the key
     */
    @API(API.Status.EXPERIMENTAL)
    static List<KeyExpression> getKeyFields(@Nonnull KeyExpression rootExpression) {
        final List<KeyExpression> normalizedKeys = rootExpression.normalizeKeyForPositions();
        if (rootExpression instanceof KeyWithValueExpression) {
            final KeyWithValueExpression keyWithValue = (KeyWithValueExpression) rootExpression;
            return new ArrayList<>(normalizedKeys.subList(0, keyWithValue.getSplitPoint()));
        } else {
            return new ArrayList<>(normalizedKeys);
        }
    }

    /**
     * Return the value fields for an expression.
     * @param rootExpression a key expression
     * @return the parts of the key expression that are in the value
     */
    @API(API.Status.EXPERIMENTAL)
    static List<KeyExpression> getValueFields(@Nonnull KeyExpression rootExpression) {
        if (rootExpression instanceof KeyWithValueExpression) {
            final List<KeyExpression> normalizedKeys = rootExpression.normalizeKeyForPositions();
            final KeyWithValueExpression keyWithValue = (KeyWithValueExpression) rootExpression;
            return new ArrayList<>(normalizedKeys.subList(keyWithValue.getSplitPoint(), normalizedKeys.size()));
        } else {
            return Collections.singletonList(EmptyKeyExpression.EMPTY);
        }
    }

    /**
     * Returns the number of version columns produced by this key expression.
     * @return the number of version columns
     */
    default int versionColumns() {
        return 0;
    }

    /**
     * Check whether a key expression uses record type key in some way.
     * @return {@code true} if record type key is used
     */
    default boolean hasRecordTypeKey() {
        return false;
    }

    /**
     * Returns a sub-set of the key expression based on the column indexes into the expression.
     * Column indexes start at zero. If {@code start} is equal to {@code end}, this
     * should return an {@link EmptyKeyExpression}, and if {@code start} is equal to zero and
     * {@code end} is equal to {@link #getColumnSize()}, then this should return the original
     * key expression.
     *
     * @param start inclusive starting column index
     * @param end exclusive ending column index
     * @return a key expression for the subkey between {@code start} and {@code end}
     * @throws com.apple.foundationdb.record.metadata.expressions.BaseKeyExpression.IllegalSubKeyException
     *     if {@code start} or {@code end} are outside the expected range or if {@code end < start}
     * @throws com.apple.foundationdb.record.metadata.expressions.BaseKeyExpression.UnsplittableKeyExpressionException
     *     if the key expression cannot be split at the current location because a split point would land in the
     *     middle of a key expression that does not support being split
     */
    @Nonnull
    KeyExpression getSubKey(int start, int end);

    /**
     * Check whether a key is a prefix of another key.
     * @param key the whole key to check
     * @return {@code true} if {@code prefix} is a left subset of {@code key}
     */
    boolean isPrefixKey(@Nonnull KeyExpression key);

    default boolean hasProperInterfaces() {
        return this instanceof KeyExpressionWithChildren || this instanceof KeyExpressionWithoutChildren;
    }

    @Nonnull
    static KeyExpression fromProto(RecordKeyExpressionProto.KeyExpression expression)
            throws DeserializationException {
        KeyExpression root = null;
        int found = 0;
        if (expression.hasField()) {
            found++;
            root = new FieldKeyExpression(expression.getField());
        }
        if (expression.hasNesting()) {
            found++;
            root = new NestingKeyExpression(expression.getNesting());
        }
        if (expression.hasThen()) {
            found++;
            root = new ThenKeyExpression(expression.getThen());
        }
        if (expression.hasList()) {
            found++;
            root = new ListKeyExpression(expression.getList());
        }
        if (expression.hasGrouping()) {
            found++;
            root = new GroupingKeyExpression(expression.getGrouping());
        }
        if (expression.hasSplit()) {
            found++;
            root = new SplitKeyExpression(expression.getSplit());
        }
        if (expression.hasEmpty()) {
            found++;
            root = EmptyKeyExpression.EMPTY;
        }
        if (expression.hasVersion()) {
            found++;
            root = VersionKeyExpression.VERSION;
        }
        if (expression.hasValue()) {
            found++;
            root = LiteralKeyExpression.fromProto(expression.getValue());
        }
        if (expression.hasFunction()) {
            found++;
            root = FunctionKeyExpression.fromProto(expression.getFunction());
        }
        if (expression.hasKeyWithValue()) {
            found++;
            root = new KeyWithValueExpression(expression.getKeyWithValue());
        }
        if (expression.hasRecordTypeKey()) {
            found++;
            root = RecordTypeKeyExpression.RECORD_TYPE_KEY;
        }
        if (expression.hasDimensions()) {
            found++;
            root = new DimensionsKeyExpression(expression.getDimensions());
        }
        if (root == null || found > 1) {
            throw new DeserializationException("Exactly one root must be specified for an index");
        }
        return root;
    }

    /**
     * Constructs a {@link KeyExpression} from a path.
     *
     * @param path The path to use for constructing the {@link KeyExpression}.
     * @return The resulting {@link KeyExpression}.
     */
    @Nonnull
    static KeyExpression fromPath(@Nonnull final List<String> path) {
        if (path.isEmpty()) {
            throw new InvalidExpressionException("attempt to create key expression using empty path");
        }
        final String fieldName = path.get(path.size() - 1);
        KeyExpression keyExpression = Key.Expressions.field(fieldName);
        final List<String> fieldPrefix = path.subList(0, path.size() - 1);
        for (int i = fieldPrefix.size() - 1; i >= 0; i --) {
            keyExpression = Key.Expressions.field(fieldPrefix.get(i)).nest(keyExpression);
        }
        return keyExpression;
    }

    /**
     * Exception thrown when there is a problem serializing a key expression.
     */
    @SuppressWarnings("serial")
    class SerializationException extends RecordCoreException {
        public SerializationException(@Nonnull String message) {
            super(message);
        }

        public SerializationException(@Nonnull String message, @Nullable Exception cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when there is a problem deserializing a key expression.
     */
    @SuppressWarnings("serial")
    class DeserializationException extends RecordCoreException {
        public DeserializationException(@Nonnull String message) {
            super(message);
        }

        public DeserializationException(@Nonnull String message, @Nullable Exception cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when there is a problem with using a key expression in a certain context.
     */
    @SuppressWarnings("serial")
    class InvalidExpressionException extends RecordCoreException {
        public InvalidExpressionException(String message) {
            super(message);
        }

        public InvalidExpressionException(String message, @Nullable Object... keyValues) {
            super(message, keyValues);
        }
    }

    /**
     * This is a runtime exception (i.e. when the expression is evaluated, not when it is being constructed)
     * that indicates that a child of an expression has returned a value that is not of the shape that is
     * expected.
     */
    @SuppressWarnings("serial")
    class InvalidResultException extends RecordCoreException {
        public InvalidResultException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when a function key expression does not have an argument.
     */
    @SuppressWarnings("serial")
    class NoSuchArgumentException extends RecordCoreException {
        public NoSuchArgumentException(String message) {
            super(message);
        }
    }
}
