/*
 * ValueMaxMatchTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Equivalence;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests maximum matching for a given {@link Value} against another {@link Value}.
 */
public class ValueMaxMatchTest {

    @Nonnull
    BiMap<Equivalence.Wrapper<Value>, Value> translate(@Nonnull final Value source,
                                                       @Nonnull final CorrelationIdentifier sourceAlias,
                                                       @Nonnull final Value target,
                                                       @Nonnull final CorrelationIdentifier targetAlias,
                                                       @Nonnull final Map<Equivalence.Wrapper<Value>, Value> valueMap) {
        final BiMap<Equivalence.Wrapper<Value>, Value> result = HashBiMap.create();
        final var boundIdentitiesMap = AliasMap.identitiesFor(source.getCorrelatedTo());
        final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(boundIdentitiesMap);
        final var translatedSource = source.replaceSubTreesPreorder(value -> valueMap.getOrDefault(boundEquivalence.wrap(value), value));
        // Now that the source is translated, we can look up the max match between the translated source
        // and the value.
        final Boolean[] descendSourceChildren = new Boolean[] {true};
        translatedSource.inPreOrder( needle -> {
            final var pulledUpSourceMap = translatedSource.pullUp(List.of(needle), boundIdentitiesMap, Set.of(), sourceAlias);
            final var toBeMappedSource = pulledUpSourceMap.get(needle);
            final var sourceIdentitiesMap = boundIdentitiesMap.combine(AliasMap.identitiesFor(Set.of(sourceAlias)));
            final Correlated.BoundEquivalence<Value> sourceIdentitiesEquivalence = new Correlated.BoundEquivalence<>(sourceIdentitiesMap);
            System.out.println("------------------------------");
            if (result.containsKey(sourceIdentitiesEquivalence.wrap(toBeMappedSource))) {
                System.out.println("retracting because I already found " + sourceIdentitiesEquivalence.wrap(toBeMappedSource) + " before!!");
                return false;
            } else {
                System.out.println("--> could not find " + sourceIdentitiesEquivalence.wrap(toBeMappedSource) + " in " + result);
            }
            System.out.println("SOURCE: visiting -> " + needle);
            target.findInPreOrder(haystack -> {
                System.out.println("   TARGET: visiting -> " + haystack);
                if (needle.semanticEquals(haystack, boundIdentitiesMap)) {

                    final var pulledUpTargetMap = target.pullUp(List.of(haystack), boundIdentitiesMap, Set.of(), targetAlias);
                    final var toBeMappedTarget = pulledUpTargetMap.get(haystack);
                    if (toBeMappedTarget == null) {
                        throw new RecordCoreException("could not pull up the target value");
                    }
                    result.put(sourceIdentitiesEquivalence.wrap(toBeMappedSource), toBeMappedTarget);
                    descendSourceChildren[0] = false;
                    System.out.println("found match for " + needle + " which is " + haystack);
                    return true; // do not descend in children, we're done
                }
                return false; // look into children, maybe we find max match there.
            });
            if (descendSourceChildren[0]) {
                System.out.println("could not find matches for " + needle + " therefor I will descend into the children");
                return true;
            } else {
                System.out.println("found match for " + needle + " returning false so we avoid delving into descendants unnecessarily");
                descendSourceChildren[0] = true;
                return false;
            }
        });
        return result;
    }

    private static final CorrelationIdentifier P = CorrelationIdentifier.of("P");
    private static final CorrelationIdentifier Q = CorrelationIdentifier.of("Q");

    private static final Value someCurrentValue = ObjectValue.of(P, getRecordType());

    @Test
    public void testValueTranslation() throws Exception {
        final var t = qov("T", getTType());
        final var t_ = qov("T'", getTType());

        final var p = rcv(
                fv(t, "a", "q"),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );

        final var p_ = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                fv(t_, "j", "q"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        final var boundIdentitiesMap = AliasMap.identitiesFor(p.getCorrelatedTo());
        final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(boundIdentitiesMap);

        final var result = translate(p, CorrelationIdentifier.of("p"), p_, CorrelationIdentifier.of("p_"), Map.of(boundEquivalence.wrap(t), t_));
        System.out.println("result is " + result);
    }

    @Test
    public void testValueTranslation2() throws Exception {
        final var t = qov("T", getTType());
        final var t_ = qov("T'", getTType());


        final var pv = rcv(
                fv(t, "a", "q"),
                fv(t, "a", "q"),
                fv(t, "a", "q"),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );

        final var p_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                fv(t_, "j", "q"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        final var boundIdentitiesMap = AliasMap.identitiesFor(pv.getCorrelatedTo());
        final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(boundIdentitiesMap);

        final var result = translate(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(boundEquivalence.wrap(t), t_));

        final var expectedBoundAliasMap = boundIdentitiesMap.combine(AliasMap.of(CorrelationIdentifier.of("p"), CorrelationIdentifier.of("p")));
        final Correlated.BoundEquivalence<Value> be = new Correlated.BoundEquivalence<>(expectedBoundAliasMap);

        // let's verify
        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var expectedMapping = Map.of(
                be.wrap(fv(p, 0)), /* -> */ fv(p_, 0, 0),
                be.wrap(fv(p, 3)), /* -> */ fv(p_, 0, 1),
                be.wrap(fv(p, 4, 0)), /* -> */ fv(p_, 1, 0),
                be.wrap(fv(p, 5)), /* -> */ fv(p_, 2));

        Assertions.assertEquals(expectedMapping, result);
    }

    @Test
    public void testValueTranslation3() throws Exception {
        final var t = qov("T", getTType());
        final var t_ = qov("T'", getTType());

        final var pv = rcv(
                add(fv(t, "a", "q"),
                        fv(t, "a", "r")),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );

        final var p_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                rcv(add(fv(t_, "a", "q"),
                                fv(t_, "a", "r")),
                        fv(t_, "b", "m")),
                fv(t_, "j", "q"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        final var boundIdentitiesMap = AliasMap.identitiesFor(pv.getCorrelatedTo());
        final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(boundIdentitiesMap);

        final var result = translate(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(boundEquivalence.wrap(t), t_));

        final var expectedBoundAliasMap = boundIdentitiesMap.combine(AliasMap.of(CorrelationIdentifier.of("p"), CorrelationIdentifier.of("p")));
        final Correlated.BoundEquivalence<Value> be = new Correlated.BoundEquivalence<>(expectedBoundAliasMap);

        // let's verify
        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var expectedMapping = Map.of(
                be.wrap(fv(p, 0)), /* -> */ fv(p_, 0, 0),
                be.wrap(fv(p, 1)), /* -> */ fv(p_, 2));

        Assertions.assertEquals(expectedMapping, result);
    }

    @Nonnull
    private static Type.Record getRecordType() {
        // (z1, (z21, (z31, c, z32)b )a, z2, (z33, e, z44)d))
        return r(f("z1"),
                f("a", r(f("z21"),
                        f("b", r("z31",
                                "c",
                                "z32")))),
                f("z2"),
                f("d", r("z33",
                        "e",
                        "z44")));
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private static Type.Record getTType() {
        return r(
                f("a", r("q", "r")),
                f("b", r("t", "m")),
                f("j", r("s", "q"))
        );
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static QuantifiedObjectValue qov(@Nonnull final String name) {
        return qov(name, getRecordType());
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static QuantifiedObjectValue qov(@Nonnull final String name, @Nonnull final Type type) {
        return QuantifiedObjectValue.of(CorrelationIdentifier.of(name), type);
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static Type.Record r(String... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .map(ValueMaxMatchTest::f)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static Type.Record r(Type.Record.Field... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static Type.Record.Field f(String name) {
        return Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of(name));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static Type.Record.Field f(String name, @Nonnull final Type type) {
        return Type.Record.Field.of(type, Optional.of(name));
    }


    private static Value rcv(Value... values) {
        return RecordConstructorValue.ofUnnamed(Arrays.stream(values).collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private static FieldValue fv(@Nonnull final Value base, String... name) {
        return fvInternal(base, name.length - 1, name);
    }

    private static FieldValue fv(String... name) {
        return fvInternal(someCurrentValue, name.length - 1, name);
    }

    private static FieldValue fv(@Nonnull final Value base, Integer... indexes) {
        return fvInternal(base, indexes.length - 1, indexes);
    }

    private static FieldValue fvInternal(Value value, int index, String... name) {
        if (index == 0) {
            return FieldValue.ofFieldNameAndFuseIfPossible(value, name[0]);
        }
        return FieldValue.ofFieldNameAndFuseIfPossible(fvInternal(value, index - 1, name), name[index]);
    }

    private static FieldValue fvInternal(Value value, int index, Integer... indexes) {
        if (index == 0) {
            return FieldValue.ofOrdinalNumber(value, indexes[0]);
        }
        return FieldValue.ofOrdinalNumberAndFuseIfPossible(fvInternal(value, index - 1, indexes), indexes[index]);
    }

    private static Value add(Value... values) {
        Verify.verify(values.length == 2);
        return new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_SS,
                values[0], values[1]);
    }
}
