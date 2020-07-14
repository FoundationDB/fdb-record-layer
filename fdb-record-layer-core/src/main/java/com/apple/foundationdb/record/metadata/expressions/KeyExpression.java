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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
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
@API(API.Status.MAINTAINED)
public interface KeyExpression extends PlanHashable {
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
        Concatenate(RecordMetaDataProto.Field.FanType.CONCATENATE),
        /**
         * Create an index value for each value of the field.
         */
        FanOut(RecordMetaDataProto.Field.FanType.FAN_OUT),
        /**
         * Nothing, only allowed with scalar fields.
         */
        None(RecordMetaDataProto.Field.FanType.SCALAR);

        private RecordMetaDataProto.Field.FanType proto;

        FanType(RecordMetaDataProto.Field.FanType fanType) {

            proto = fanType;
        }

        RecordMetaDataProto.Field.FanType toProto() {
            return proto;
        }

        public static FanType valueOf(RecordMetaDataProto.Field.FanType fanType) throws DeserializationException {
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
    RecordMetaDataProto.KeyExpression toKeyExpression();

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
     * Flatten this key expression into a list of {@link Element}s, much like {@link #normalizeKeyForPositions()}.
     * By default, this method throws an exception because most key expressions cannot be flattened to a list of
     * elements without prior adjustment. This method is only overriden by key expressions that can be flattened.
     * @return a list of elements representing this key expression in unnested form
     * @see ElementKeyExpression#flattenForPlanner()
     * @see ThenKeyExpression#flattenForPlanner()
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    default List<Element> flattenForPlanner() {
        throw new RecordCoreException("illegal non-element expression");
    }

    /**
     * Normalize this key expression into another key expression that pushes all nesting and fan-out to
     * {@link ElementKeyExpression}s at the leaves.
     *
     * <p>
     * By default, a key expression is a complicated nested structure that can be difficult to work with. This method
     * pushes much of the complexity, including nested structures and fan-out of repeated fields, to special key
     * expressions that track these relationship using the {@link Source} abstraction. This pre-processing makes
     * planning nested and repeated structures much simpler.
     * </p>
     *
     * <p>
     * This normalization process requires tracking some state since the name of a nested field is available
     * only at the relevant {@link FieldKeyExpression}, but that information is necessary to construct the
     * {@link ElementKeyExpression} at the leaves of the sub-tree rooted at the {@link NestingKeyExpression}. This
     * extra information is tracked in the {@code fieldNamePrefix}.
     * </p>
     * @param source the source representing the input stream of the key expression
     * @param fieldNamePrefix the (non-repeated) field names on the path from the most recent source to this part of the key expression
     * @return a new key expression that has only {@link ElementKeyExpression}s at its leaves
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    KeyExpression normalizeForPlanner(@Nonnull Source source, @Nonnull List<String> fieldNamePrefix);

    /**
     *
     * Grab the key fields for an expression.
     *
     * @return KeyExpressions
     */
    @API(API.Status.EXPERIMENTAL)
    default List<KeyExpression> getKeyFields() {
        return new ArrayList<>(normalizeKeyForPositions());
    }

    /**
     * Grab the value fields for an expression
     *
     * @return KeyExpressions
     */
    @API(API.Status.EXPERIMENTAL)
    default List<KeyExpression> getValueFields() {
        return Collections.singletonList(EmptyKeyExpression.EMPTY);
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
     * Returns a sub-set of the key expression.
     * @param start starting position
     * @param end ending position
     * @return a key expression for the subkey between {@code start} and {@code end}
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
    static KeyExpression fromProto(RecordMetaDataProto.KeyExpression expression)
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
        if (root == null || found > 1) {
            throw new DeserializationException("Exactly one root must be specified for an index");
        }
        return root;
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
