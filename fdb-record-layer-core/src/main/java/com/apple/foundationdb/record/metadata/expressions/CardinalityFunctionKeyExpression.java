/*
 * CardinalityFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023-2030 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
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
 * {@snippet java: function("cardinality", field("ARRAY_FIELD").nest(field("values", KeyExpression.FanType.Concatenate)))}
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
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> rec, @Nullable Message message, @Nonnull Key.Evaluated argvals) {
        // For the nullable array wrapper pattern, `field(…).nest(field("values", Concatenate))`,
        // detect whether the outer wrapper message field is absent and return NULL in that case.
        // Without this check, an absent wrapper (NULL array) and a present-but-empty wrapper (empty array) both
        // produce an empty list as the argument, and would both incorrectly yield cardinality 0.
        if (arguments instanceof NestingKeyExpression) {
            final FieldKeyExpression parent = ((NestingKeyExpression)arguments).getParent();
            if (parent.getNullStandin() != Key.Evaluated.NullStandin.NOT_NULL) {
                if (message == null) {
                    return Collections.singletonList(Key.Evaluated.NULL);
                }
                final Descriptors.FieldDescriptor fieldDescriptor =
                        message.getDescriptorForType().findFieldByName(parent.getFieldName());
                if (fieldDescriptor != null && !message.hasField(fieldDescriptor)) {
                    return Collections.singletonList(Key.Evaluated.NULL);
                }
            }
        }

        // `argvals` will contain a single element, which is the array as a `java.util.List`.
        // Note: The current mechanism materializes the argument array into a `Key.Evaluated`, even though we only
        // really need its element count. This could be optimized when an ARRAY field is accessed directly. To this end
        // we would have to detect the child `FieldKeyExpression`, get a hold of the appropriate `FieldDescriptor`,
        // and then invoke `Message#getRepeatedFieldCount()` directly (instead of `getField()`, which builds the list).
        Verify.verify(argvals.size() == 1);
        final Object arg = argvals.getObject(0);
        if (arg == null) {
            return Collections.singletonList(Key.Evaluated.NULL);
        }
        Verify.verify(arg instanceof List<?>);
        final var argList = (List<?>)arg;
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
            return List.of(
                    new FunctionKeyExpression.BiFunctionBuilder(FunctionNames.CARDINALITY, CardinalityFunctionKeyExpression::new)
            );
        }
    }
}
