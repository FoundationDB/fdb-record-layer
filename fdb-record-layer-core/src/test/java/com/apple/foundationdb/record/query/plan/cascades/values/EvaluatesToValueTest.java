/*
 * EvaluatesToValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Verify;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;

import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.newCov;


public class EvaluatesToValueTest {

    @Nonnull
    private static EvaluationContext replaceBinding(@Nonnull final EvaluationContext evaluationContext,
                                                    @Nonnull final String constantId,
                                                    @Nullable Object newValue) {
        final var list = evaluationContext.getBindings().asMappingList();
        Verify.verify(list.size() == 1);
        final var bindingValueMap = new HashMap<String, Object>();
        bindingValueMap.put(constantId, newValue);
        return EvaluationContext.forBindings(Bindings.newBuilder().set(list.get(0).getKey(), bindingValueMap).build());
    }

    @Test
    void evaluatesToTrueWorksCorrectly() {
        final var valueWrapper = newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
        final var cov = (ConstantObjectValue)valueWrapper.value();
        final var evaluationContext = valueWrapper.getEvaluationContext();
        final var valueUnderTest = EvaluatesToValue.of(cov, evaluationContext);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(evaluationContext));
        Assertions.assertTrue(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(evaluationContext)));

        var newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), false);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));

        newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), null);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));
    }

    @Test
    void evaluatesToFalseWorksCorrectly() {
        final var valueWrapper = newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
        final var cov = (ConstantObjectValue)valueWrapper.value();
        final var evaluationContext = valueWrapper.getEvaluationContext();
        final var valueUnderTest = EvaluatesToValue.of(cov, evaluationContext);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(evaluationContext));
        Assertions.assertTrue(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(evaluationContext)));

        var newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), true);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));

        newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), null);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));
    }

    @Test
    void evaluatesToNullWorksCorrectly() {
        final var valueWrapper = newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), null);
        final var cov = (ConstantObjectValue)valueWrapper.value();
        final var evaluationContext = valueWrapper.getEvaluationContext();
        final var valueUnderTest = EvaluatesToValue.of(cov, evaluationContext);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(evaluationContext));
        Assertions.assertTrue(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(evaluationContext)));

        var newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), true);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));

        newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), false);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));
    }

    @Test
    void evaluatesToNotNullWorksCorrectly() {
        final var valueWrapper = newCov(Type.primitiveType(Type.TypeCode.INT), 42);
        final var cov = (ConstantObjectValue)valueWrapper.value();
        final var evaluationContext = valueWrapper.getEvaluationContext();
        final var valueUnderTest = EvaluatesToValue.of(cov, evaluationContext);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(evaluationContext));
        Assertions.assertTrue(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(evaluationContext)));

        var newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), 43);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertTrue(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));

        newEvaluationContext = replaceBinding(evaluationContext, cov.getConstantId(), null);
        Assertions.assertNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext));
        Assertions.assertFalse(Verify.verifyNotNull(valueUnderTest.evalWithoutStore(newEvaluationContext)));
    }
}
