/*
 * InsertExpressionTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link InsertExpression}.
 */
class InsertExpressionTest {

    private static Type.Record recordTypeWith(final Type.TypeCode fieldType) {
        return Type.Record.fromFields(List.of(
                Type.Record.Field.of(Type.primitiveType(fieldType), Optional.of("a"))));
    }

    private static Quantifier.ForEach scanQuantifierOver(final Type flowedType, final String recordType) {
        final var scan = new FullUnorderedScanExpression(ImmutableSet.of(recordType), flowedType, new AccessHints());
        return Quantifier.forEach(Reference.initialOf(scan));
    }

    @Test
    void toStringTranslatesEscapedProtoIdentifierBackToUserIdentifier() {
        final var targetType = recordTypeWith(Type.TypeCode.LONG);
        final var escapedTargetRecordType = ProtoUtils.toProtoBufCompliantName("My.Record");
        final var insertExpression = new InsertExpression(scanQuantifierOver(targetType, "R"), escapedTargetRecordType, targetType);

        assertEquals("Insert(My.Record)", insertExpression.toString());
    }
}
