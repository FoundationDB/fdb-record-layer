/*
 * CardinalityFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A key expression representing the {@code CARDINALITY(<expr>)} function.
 *
 * <p>When CARDINALITY() is applied to a nullable array column, the repeated field will be wrapped on the Protobuf level
 * and the key expression will look like this:
 * {@code: function("cardinality", field("ARRAY_FIELD").nest(field("values", KeyExpression.FanType.Concatenate)))}
 * … where {@code "values"} is the repeated field of the appropriate array element type, and the {@code Concatenate}
 * fan-out type collects all array elements into a single {@code Key.Evaluated} holding the array.
 */
@API(API.Status.EXPERIMENTAL)
public class CardinalityFunctionKeyExpression extends FunctionKeyExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Cardinality-Function");

    protected CardinalityFunctionKeyExpression(@Nonnull String name, @Nonnull KeyExpression arguments) {
        super(name, arguments);
    }

    @Override
    public int getMinArguments() {
        return 1;
    }

    @Override
    public int getMaxArguments() {
        return 1;
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final List<? extends Value> argumentValues) {
        return resolveAndEncapsulateFunction(getName(), argumentValues);
    }

    @Nonnull
    private static List<Key.Evaluated> getNullResult() {
        return Collections.singletonList(Key.Evaluated.NULL);
    }

    /**
     * Evaluates the cardinality against a record or protobuf message. This method handles the SQL-NULL cases and, where
     * possible, counts the repeated Protobuf field directly via {@link Message#getRepeatedFieldCount} to avoid the
     * materialization of the argument array into a {@link Key.Evaluated}.
     *
     * <p>Two argument shapes are recognized and fast-pathed:
     * <ul>
     * <li>{@code field("arr", Concatenate)} — A {@code NOT NULL} array column.</li>
     * <li>{@code field("arr").nest(field("values", Concatenate))} — The nullable-array wrapper pattern
     *     (see {@link #evaluateOnWrappedArray}).</li>
     * </ul>
     * Anything else (for example, deeper nestings like {@code field("struct").nest(field("arr", Concatenate))})
     * falls back to {@code super.evaluateMessage}.
     */
    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record,
                                                                   @Nullable Message message) {
        if (message == null) {
            // Without a record, there is nothing to count; the cardinality is SQL NULL.
            return getNullResult();
        } else if (arguments instanceof NestingKeyExpression nesting) {
            if (NullableArrayTypeUtils.matchArrayWrapper(nesting).isPresent()) {
                return evaluateOnWrappedArray(message, nesting.getParent());
            }
        } else if (arguments instanceof FieldKeyExpression field) {
            final Descriptors.FieldDescriptor descriptor =
                    message.getDescriptorForType().findFieldByName(field.getFieldName());
            if (descriptor != null && descriptor.isRepeated()) {
                final int cardinality = message.getRepeatedFieldCount(descriptor);
                return Collections.singletonList(Key.Evaluated.scalar(cardinality));
            }
        }
        return super.evaluateMessage(record, message);
    }

    /**
     * Fast path to evaluate the cardinality for the nullable-array wrapper pattern.
     */
    @Nonnull
    private static List<Key.Evaluated> evaluateOnWrappedArray(@Nonnull Message message,
                                                              @Nonnull FieldKeyExpression wrapper) {
        final Descriptors.FieldDescriptor wrapperDescriptor =
                message.getDescriptorForType().findFieldByName(wrapper.getFieldName());
        if (wrapperDescriptor == null) {
            // Metadata mismatch; surface SQL NULL rather than throwing.
            return getNullResult();
        }
        // If the wrapper sub-field is unset on this record, yield NULL, unless a NOT_NULL standin dictates that the
        // unset wrapper should be treated as the default value of the type, i.e., an empty wrapper.
        if (!message.hasField(wrapperDescriptor)) {
            if (wrapper.getNullStandin() == Key.Evaluated.NullStandin.NOT_NULL) {
                return Collections.singletonList(Key.Evaluated.scalar(0));
            } else {
                return getNullResult();
            }
        }
        final Message wrapperMessage = (Message)message.getField(wrapperDescriptor);
        final Descriptors.FieldDescriptor valuesDescriptor =
                wrapperMessage.getDescriptorForType().findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName());
        Verify.verifyNotNull(valuesDescriptor);
        Verify.verify(valuesDescriptor.isRepeated());
        final int cardinality = wrapperMessage.getRepeatedFieldCount(valuesDescriptor);
        return Collections.singletonList(Key.Evaluated.scalar(cardinality));
    }

    /**
     * Evaluates the cardinality for argument shapes that {@link #evaluateMessage} did not recognize.
     */
    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> rec,
                                                                    @Nullable Message message,
                                                                    @Nonnull Key.Evaluated argvals) {
        // The NULL case has already been handled in `evaluateMessage()`, so at this point, `argvals` contains a
        // single element, the array as a `java.util.List`, and the array itself is never null.
        Verify.verify(argvals.size() == 1);
        final List<?> argList = (List<?>)Verify.verifyNotNull(argvals.getObject(0));
        return Collections.singletonList(Key.Evaluated.scalar(argList.size()));
    }

    /// @see FunctionKeyExpression#create
    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        // Note: `arguments.getColumnSize()` must be 1, but `create()` already validates that.
        if (arguments.createsDuplicates()) {
            throw new InvalidExpressionException("The CARDINALITY() argument must produce a single value.");
        }
        return super.validate(descriptor);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return super.basePlanHash(mode, BASE_HASH);
    }

    /**
     * Factory for {@link CardinalityFunctionKeyExpression}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    @API(API.Status.EXPERIMENTAL)
    public static class CardinalityFunctionKeyExpressionFactory implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return ImmutableList.of(
                    new FunctionKeyExpression.BiFunctionBuilder(FunctionNames.CARDINALITY,
                            CardinalityFunctionKeyExpression::new)
            );
        }
    }
}
