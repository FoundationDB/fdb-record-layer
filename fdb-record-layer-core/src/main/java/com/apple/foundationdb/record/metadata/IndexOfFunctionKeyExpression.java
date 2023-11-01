/*
 * IndexOfFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Function key expression that computes the first index of an item within a list. This key expression takes
 * two arguments:
 *
 * <ol>
 *     <li>A list of items.</li>
 *     <li>A scalar object.</li>
 * </ol>
 *
 * <p>
 * It returns a single column, which will contain the (zero-indexed) position of the scalar object
 * within the list or {@code null} if the element is not in the list. For example, on a message like:
 * </p>
 *
 * <pre>{@code
 * message WithRepeatedField {
 *     repeated int64 foo = 1;
 *     int64 bar = 2;
 * }
 * }</pre>
 *
 * <p>
 * Then the expression <code>function({@value #NAME}, concat(field("foo", FanType.Concatenate), field("bar")))</code>
 * would return the first position of the <code>bar</code> field within the <code>foo</code> field.
 * </p>
 */
@SuppressWarnings("squid:S1845") // allow constant NAME to shadow name field in abstract parent
@API(API.Status.INTERNAL)
public class IndexOfFunctionKeyExpression extends FunctionKeyExpression {
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("IndexOf-Function-Key-Expression");
    @Nonnull
    public static final String NAME = "index_of";

    private IndexOfFunctionKeyExpression(KeyExpression arguments) {
        super(NAME, arguments);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.planHash(hashMode, BASE_HASH, getArguments());
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, getArguments());
    }

    @Override
    public int getMinArguments() {
        return 2;
    }

    @Override
    public int getMaxArguments() {
        return 2;
    }

    @Override
    public boolean needsCopyingToPartialRecord() {
        return false;
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> rec, @Nullable final Message message, @Nonnull final Key.Evaluated arguments) {
        List<?> l = arguments.getObject(0, List.class);
        if (l == null) {
            return List.of(Key.Evaluated.NULL);
        }
        Object elem = arguments.getObject(1);
        int index = l.indexOf(elem);
        return List.of(index < 0 ? Key.Evaluated.NULL : Key.Evaluated.scalar(index));
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    /**
     * Factory class of {@link IndexOfFunctionKeyExpression}s.
     *
     * @see FunctionKeyExpression.Factory
     * @see FunctionKeyExpression.Registry
     */
    // @AutoService(Factory.class)
    @API(API.Status.INTERNAL)
    public static class IndexOfFunctionKeyExpressionFactory implements Factory {
        private static final List<FunctionKeyExpression.Builder> BUILDERS = List.of(new Builder());

        static class Builder extends FunctionKeyExpression.Builder {
            public Builder() {
                super(NAME);
            }

            @Nonnull
            @Override
            public IndexOfFunctionKeyExpression build(@Nonnull final KeyExpression arguments) {
                return new IndexOfFunctionKeyExpression(arguments) ;
            }
        }

        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return BUILDERS;
        }
    }
}
