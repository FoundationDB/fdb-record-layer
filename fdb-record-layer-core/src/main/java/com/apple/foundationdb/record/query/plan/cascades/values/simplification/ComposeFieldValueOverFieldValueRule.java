/*
 * ComposeFieldValueOverFieldValueRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.anyObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * A rule that composes a field access and an underlying field access to a concatenated field access.
 * <br>
 * {@code (_.a).b} or more precisely {@code FieldValue(FieldValue(_, "a"), "b")} is transformed to {@code _.a.b} or
 * {@code FieldValue(_, ["a", "b"])}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ComposeFieldValueOverFieldValueRule extends ValueSimplificationRule<FieldValue> {
    @Nonnull
    private static final BindingMatcher<Value> innerChildMatcher = anyValue();
    @Nonnull
    private static final CollectionMatcher<Integer> innerFieldPathOrdinalsMatcher = all(anyObject());

    @Nonnull
    private static final CollectionMatcher<Type> innerFieldPathTypesMatcher = all(anyObject());

    @Nonnull
    private static final BindingMatcher<FieldValue> innerFieldValueMatcher =
            ValueMatchers.fieldValueWithFieldPath(innerChildMatcher, innerFieldPathOrdinalsMatcher, innerFieldPathTypesMatcher);

    @Nonnull
    private static final CollectionMatcher<Integer> outerFieldPathOrdinalsMatcher = all(anyObject());

    @Nonnull
    private static final CollectionMatcher<Type> outerFieldPathTypesMatcher = all(anyObject());

    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher =
            ValueMatchers.fieldValueWithFieldPath(innerFieldValueMatcher, outerFieldPathOrdinalsMatcher, outerFieldPathTypesMatcher);

    public ComposeFieldValueOverFieldValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();

        final var grandChild = bindings.get(innerChildMatcher);
        final var innerFieldPathOrdinals = bindings.get(innerFieldPathOrdinalsMatcher);
        final var innerFieldPathTypes = bindings.get(innerFieldPathTypesMatcher);
        Verify.verify(!innerFieldPathOrdinals.isEmpty());
        Verify.verify(!innerFieldPathTypes.isEmpty());
        final var outer = bindings.get(rootMatcher);
        final var child = outer.getChild();
        final var outerFieldPathOrdinals = bindings.get(outerFieldPathOrdinalsMatcher);
        final var outerFieldPathTypes = bindings.get(outerFieldPathTypesMatcher);
        Verify.verify(child instanceof FieldValue);
        Verify.verify(!outerFieldPathOrdinals.isEmpty());
        Verify.verify(!outerFieldPathTypes.isEmpty());
        call.yieldResult(FieldValue.ofFields(grandChild, ((FieldValue)(child)).getFieldPath().withSuffix(outer.getFieldPath())));
    }
}
