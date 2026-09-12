/*
 * LiteralsTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test suite for {@link Literals}: that no {@link OrderedLiteral}(s) are skipped when building one, and that a literal
 * bound to {@code NULL} stays distinct from a <em>value-free</em> literal. Both have a null literal object; only the
 * latter contributes no binding, which is what leaves its constant id unbound so that a plan built without a value can
 * be reused once a value is bound.
 */
public class LiteralsTests {

    private static final int ARGUMENTS_SIZE = 100;

    private static final Type LONG_TYPE = Type.primitiveType(Type.TypeCode.LONG, false);
    private static final Type STRING_TYPE = Type.primitiveType(Type.TypeCode.STRING);

    @Nonnull
    private final Random random = new Random(42L);

    @Test
    void nullLiteralsAreNotExcluded() {
        // TODO use RandomizedTestUtils.
        final var tokenIndices = generateUniqueIntegers(random, 0, 1000, ARGUMENTS_SIZE);
        final var nullLiteralsCount = 1 + random.nextInt(ARGUMENTS_SIZE - 1); // at least 1 null literal.
        final ArrayList<OrderedLiteral> orderedLiterals = new ArrayList<>(ARGUMENTS_SIZE);
        final var expectedNullOrderedLiterals = ImmutableList.<OrderedLiteral>builderWithExpectedSize(nullLiteralsCount);
        final var expectedNonNullOrderedLiterals = ImmutableList.<OrderedLiteral>builderWithExpectedSize(ARGUMENTS_SIZE - nullLiteralsCount);
        int nullLiteralsAdded = 0;
        for (final var tokenIndex : tokenIndices) {
            if (nullLiteralsAdded == nullLiteralsCount) {
                final var nonNullLiteral = nonNullLiteral(tokenIndex, random.nextInt() % 10000);
                orderedLiterals.add(nonNullLiteral);
                expectedNonNullOrderedLiterals.add(nonNullLiteral);
            } else {
                final var nullLiteral = nullLiteral(tokenIndex);
                orderedLiterals.add(nullLiteral(tokenIndex));
                expectedNullOrderedLiterals.add(nullLiteral);
                nullLiteralsAdded++;
            }
        }
        Collections.shuffle(orderedLiterals);

        final var literalsBuilder = Literals.newBuilder();
        orderedLiterals.forEach(literalsBuilder::addLiteral);
        final var actualLiterals = literalsBuilder.build();
        final var actualBindings = actualLiterals.asBindings();

        final HashMap<String, Object> expectedMap = new HashMap<>();
        expectedNullOrderedLiterals.build().forEach(l -> expectedMap.put(l.getConstantId(), l.getLiteralObject()));
        expectedNonNullOrderedLiterals.build().forEach(l -> expectedMap.put(l.getConstantId(), l.getLiteralObject()));

        Assertions.assertEquals(expectedMap, actualBindings);
    }

    @Test
    void bindingsExcludeValueFreeLiteral() {
        final var builder = Literals.newBuilder();
        final var valueFree = builder.addValueFreeLiteral(LONG_TYPE, "param_a", 1);
        final var bound = builder.addLiteral(STRING_TYPE, "hello", null, "param_b", 2);
        final var literals = builder.build();

        // The value-free literal occupies the table, it just does not bind.
        assertThat(literals.getOrderedLiterals()).hasSize(2);
        assertThat(literals.asBindings()).containsOnlyKeys(bound.getConstantId());
        assertThat(literals.isValueFree(valueFree.getConstantId())).isTrue();
        assertThat(literals.isValueFree(bound.getConstantId())).isFalse();
    }

    @Test
    void bindingsIncludeLiteralBoundToNull() {
        final var builder = Literals.newBuilder();
        final var boundToNull = builder.addLiteral(LONG_TYPE, null, null, "param_a", 1);
        final var valueFree = builder.addValueFreeLiteral(LONG_TYPE, "param_b", 2);
        final var literals = builder.build();

        // A present key with a null value means "bound to SQL NULL"; an absent key means "no value at all".
        assertThat(literals.asBindings()).containsKey(boundToNull.getConstantId());
        assertThat(literals.asBindings().get(boundToNull.getConstantId())).isNull();
        assertThat(literals.asBindings()).doesNotContainKey(valueFree.getConstantId());
        assertThat(literals.isValueFree(boundToNull.getConstantId())).isFalse();
    }

    @Test
    void literalOfTellsBoundValueFreeAndAbsentApart() {
        final var builder = Literals.newBuilder();
        final var boundToNull = builder.addLiteral(LONG_TYPE, null, null, "param_a", 1);
        final var valueFree = builder.addValueFreeLiteral(LONG_TYPE, "param_b", 2);
        final var literals = builder.build();

        // All three states are distinguishable from one lookup, which asBindings() cannot do: there, a null binding and
        // a value-free literal both read as an absent value.
        assertThat(literals.literalOf(boundToNull.getConstantId())).contains(boundToNull);
        assertThat(literals.literalOf(valueFree.getConstantId())).contains(valueFree);
        assertThat(literals.literalOf("c999")).isEmpty();
        assertThat(literals.isValueFree("c999")).isFalse();
    }

    @Test
    void evaluationContextLeavesValueFreeConstantUnbound() {
        final var builder = Literals.newBuilder();
        final var valueFree = builder.addValueFreeLiteral(LONG_TYPE, "param_a", 1);
        final var bound = builder.addLiteral(STRING_TYPE, "hello", null, "param_b", 2);
        final var literals = builder.build();

        // Mirrors what QueryExecutionContext.getEvaluationContext does with these bindings. The case where *every*
        // literal is value-free, and so no constant may be bound at all, is exercised through the real path in
        // OfflineValueFreePlanGenerationTest.
        final var evaluationContext = EvaluationContext.newBuilder()
                .setConstant(Quantifier.constant(), literals.asBindings())
                .build(ParseHelpers.EMPTY_TYPE_REPOSITORY);

        assertThat(evaluationContext.containsConstantBinding(Quantifier.constant(), bound.getConstantId())).isTrue();
        assertThat(evaluationContext.containsConstantBinding(Quantifier.constant(), valueFree.getConstantId())).isFalse();
    }

    @Test
    void importLiteralsRetainsValueFreeLiteral() {
        final var source = Literals.newBuilder();
        source.addValueFreeLiteral(LONG_TYPE, "param_a", 1);
        final var sourceLiterals = source.build();

        final var target = Literals.newBuilder();
        final var imported = target.importLiteralsRetrieveNewLiterals(sourceLiterals);

        // This is the hop a compiled function body's literals make into the enclosing query's context.
        assertThat(imported).hasSize(1);
        assertThat(imported.get(0).isValueFree()).isTrue();
        assertThat(target.build().isValueFree(imported.get(0).getConstantId())).isTrue();
        assertThat(target.build().asBindings()).isEmpty();

        // Re-importing the same table is a no-op, not a conflict: the same function may be invoked more than once.
        assertThat(target.importLiteralsRetrieveNewLiterals(sourceLiterals)).isEmpty();
    }

    @Test
    void importLiteralsWithConflictingValueFreenessThrows() {
        // Same constant id, one with a value and one without. Both literal objects are null, so only the value-free flag
        // distinguishes them; skipping the import silently would drop a real binding.
        final var source = Literals.newBuilder();
        source.addLiteral(LONG_TYPE, null, null, "param_a", 1);
        final var sourceLiterals = source.build();

        final var target = Literals.newBuilder();
        target.addValueFreeLiteral(LONG_TYPE, "param_a", 1);

        assertThatThrownBy(() -> target.importLiteralsRetrieveNewLiterals(sourceLiterals))
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("conflicting literals");
    }

    @Test
    void getFirstValueDuplicateMaybeIgnoresValueFreeLiteral() {
        final var builder = Literals.newBuilder();
        builder.addValueFreeLiteral(LONG_TYPE, "param_a", 1);
        final var boundToNull = builder.addLiteral(LONG_TYPE, null, null, "param_b", 2);

        // A value-free literal is never registered for reverse lookup, so it cannot become the deduplication target for
        // a literal genuinely bound to NULL.
        assertThat(builder.getFirstValueDuplicateMaybe(null)).contains(boundToNull);
    }

    @Test
    void toProtoOnValueFreeLiteralThrows() {
        final var valueFree = Literals.newBuilder().addValueFreeLiteral(LONG_TYPE, "param_a", 1);

        // The wire format encodes an absent value as NULL, so serializing a value-free literal would silently turn it
        // into one bound to NULL. Only value-free planning produces these, and it never continues a query.
        assertThatThrownBy(() -> valueFree.toProto(PlanSerializationContext.newForCurrentMode(), 0))
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("value-free");
    }

    @Test
    void toStringOnValueFreeLiteralShowsDeclaredTypeInPlaceOfValue() {
        final var builder = Literals.newBuilder();
        final var valueFree = builder.addValueFreeLiteral(LONG_TYPE, "param_a", 12);

        // There is no value to render, so the declared type stands in for it, alongside the constant id the parameter
        // reserved -- which is what a "Missing binding" message names when such a constant is dereferenced.
        assertThat(valueFree).hasToString("?param_a:{" + LONG_TYPE + "}@" + valueFree.getConstantId());
        assertThat(valueFree.getConstantId()).isEqualTo("c12");
    }

    @Test
    void toStringOnValueFreeLiteralIncludesScope() {
        final var builder = Literals.newBuilder();
        builder.setScope("F1");
        final var valueFree = builder.addValueFreeLiteral(LONG_TYPE, "param_a", 12);

        // Literals compiled inside a function body are namespaced by function name, so two functions may each have a
        // parameter at the same token index.
        assertThat(valueFree).hasToString("?param_a:{" + LONG_TYPE + "}@cF112");
    }

    @Test
    void toStringOnLiteralBoundToNullIsUnchanged() {
        final var boundToNull = Literals.newBuilder().addLiteral(LONG_TYPE, null, null, "param_a", 12);

        // A bound named parameter renders as just its name, whether or not its value happens to be NULL. Only the
        // value-free form carries the type, so the two are distinguishable in a log or a debugger.
        assertThat(boundToNull).hasToString("?param_a");
    }

    @Nonnull
    private static OrderedLiteral nonNullLiteral(int tokenIndex, int value) {
        return new OrderedLiteral(Type.primitiveType(Type.TypeCode.LONG), value, null, null, tokenIndex, Optional.empty());
    }

    @Nonnull
    private static OrderedLiteral nullLiteral(int tokenIndex) {
        return new OrderedLiteral(Type.primitiveType(Type.TypeCode.NULL), null /*literal value*/, null, null, tokenIndex, Optional.empty());
    }

    @Nonnull
    public static Set<Integer> generateUniqueIntegers(@Nonnull final Random random, int min, int max, int count) {
        Assert.thatUnchecked(0 <= min && min < max, "invalid range boundaries");
        Assert.thatUnchecked(count > 0 && count <= (max - min), "pick values' range is smaller than the desired number of unique values");
        return random.ints(min, max + 1)
                .distinct()
                .limit(count)
                .boxed()
                .collect(ImmutableSet.toImmutableSet());
    }
}
