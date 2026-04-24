/*
 * VersionFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Function representing access to the row version of a record. This should be superseded by using the
 * {@link PseudoField#ROW_VERSION} directly during queries, but older queries may still use
 * this function.
 */
@AutoService(BuiltInFunction.class)
public class VersionFunction extends BuiltInFunction<Value> {
    public VersionFunction() {
        super("version",
                List.of(Type.any()),
                (ignored, arguments) -> encapsulateUnnamed(arguments));
    }

    @Nonnull
    private static Value encapsulateUnnamed(@Nonnull final List<? extends Typed> arguments) {
        final var childRecordValue = Iterables.getOnlyElement(arguments);
        return FieldValue.ofFieldNameAndFuseIfPossible((Value) childRecordValue, PseudoField.ROW_VERSION.getFieldName());
    }
}
