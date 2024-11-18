/*
 * ValueTranslationTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.google.common.base.Verify;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

/**
 * Tests for value mapping.
 */
public class ValueTranslationTest {
    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record getTType() {
        return r(
                f("a", r("q", "r")),
                f("b", r("t", "m")),
                f("j", r("s", "q"))
        );
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type getSType() {
        return Type.primitiveType(Type.TypeCode.INT);
    }

    @Nonnull
    private Type getUType() {
        return Type.primitiveType(Type.TypeCode.INT);
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private QuantifiedObjectValue qov(@Nonnull final CorrelationIdentifier name, @Nonnull final Type type) {
        return QuantifiedObjectValue.of(name, type);
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record r(String... fieldNames) {
        return Type.Record
                .fromFields(Arrays.stream(fieldNames)
                        .map(this::f)
                        .collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record r(Type.Record.Field... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record.Field f(String name) {
        return Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of(name));
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record.Field f(String name, @Nonnull final Type type) {
        return Type.Record.Field.of(type, Optional.of(name));
    }

    @Nonnull
    private Value rcv(Value... values) {
        return RecordConstructorValue.ofUnnamed(Arrays.stream(values).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    private Value rcv(boolean isNullable, Object... valuesAndNames) {
        final var columnsBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < valuesAndNames.length - 1; i += 2) {
            final var value = (Value)Objects.requireNonNull(valuesAndNames[i]);
            final var fieldName = (String)Objects.requireNonNull(valuesAndNames[i + 1]);
            columnsBuilder.add(Column.of(Optional.of(fieldName), value));
        }

        return RecordConstructorValue.ofColumns(columnsBuilder.build(), isNullable);
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private FieldValue fv(@Nonnull final Value base, String... name) {
        return fvInternal(base, name.length - 1, name);
    }

    @Nonnull
    private FieldValue fv(@Nonnull final Value base, Integer... indexes) {
        return fvInternal(base, indexes.length - 1, indexes);
    }

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier tAlias = CorrelationIdentifier.of("T");

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier t_Alias = CorrelationIdentifier.of("T'");

    @SuppressWarnings("checkstyle:MemberName")
    private final QuantifiedObjectValue t = qov(tAlias, getTType());

    @SuppressWarnings("checkstyle:MemberName")
    private final QuantifiedObjectValue t_ = qov(t_Alias, getTType());

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier mAlias = CorrelationIdentifier.of("M");

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier m_Alias = CorrelationIdentifier.of("M'");

    @SuppressWarnings("checkstyle:MemberName")
    private final QuantifiedObjectValue m = qov(mAlias, getMType());

    @SuppressWarnings("checkstyle:MemberName")
    private final QuantifiedObjectValue m_ = qov(m_Alias, getMType());

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier nAlias = CorrelationIdentifier.of("N");

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier n_Alias = CorrelationIdentifier.of("N'");

    @SuppressWarnings("checkstyle:MemberName")
    private final QuantifiedObjectValue n = qov(nAlias, getNType());

    @SuppressWarnings("checkstyle:MemberName")
    private final QuantifiedObjectValue n_ = qov(n_Alias, getNType());

    @SuppressWarnings("checkstyle:MemberName")
    final CorrelationIdentifier sAlias = CorrelationIdentifier.of("S");

    @SuppressWarnings("checkstyle:MemberName")
    final CorrelationIdentifier s_Alias = CorrelationIdentifier.of("S'");

    @SuppressWarnings("checkstyle:MemberName")
    final QuantifiedObjectValue s = qov(sAlias, getSType());

    @SuppressWarnings("checkstyle:MemberName")
    final QuantifiedObjectValue s_ = qov(s_Alias, getSType());

    @SuppressWarnings("checkstyle:MemberName")
    final CorrelationIdentifier uAlias = CorrelationIdentifier.of("U");

    @SuppressWarnings("checkstyle:MemberName")
    final CorrelationIdentifier u_Alias = CorrelationIdentifier.of("U'");

    @SuppressWarnings("checkstyle:MemberName")
    final QuantifiedObjectValue u = qov(uAlias, getUType());

    @SuppressWarnings("checkstyle:MemberName")
    final QuantifiedObjectValue u_ = qov(u_Alias, getUType());

    @Nonnull
    private FieldValue fvInternal(Value value, int index, String... name) {
        if (index == 0) {
            return FieldValue.ofFieldNameAndFuseIfPossible(value, name[0]);
        }
        return FieldValue.ofFieldNameAndFuseIfPossible(fvInternal(value, index - 1, name), name[index]);
    }

    @Nonnull
    private FieldValue fvInternal(Value value, int index, Integer... indexes) {
        if (index == 0) {
            return FieldValue.ofOrdinalNumber(value, indexes[0]);
        }
        return FieldValue.ofOrdinalNumberAndFuseIfPossible(fvInternal(value, index - 1, indexes), indexes[index]);
    }

    @Nonnull
    private Value add(Value... values) {
        Verify.verify(values.length == 2);
        return new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_II, values[0], values[1]);
    }

    @Test
    public void testMultiLevelValueTranslation() {
        /*
             1st level:
             (t.a.q, t.a.r, (t.b.t), t.j.s)      ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>
         */

        final var pv = rcv(
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

        /*
           translation of (t.a.q, t.a.r, (t.b.t), t.j.s) with correlation mapping of t -> t' (and no m3) should merely
           replace t with t', i.e. the result should be (t'.a.q, t'.a.r, (t'.b.t), t'.j.s)
         */

        final var l1TranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1TranslatedQueryValue = pv.translateCorrelations(l1TranslationMap, true);
        final var expectedL1TranslatedQueryValue = rcv(
                fv(t_, "a", "q"),
                fv(t_, "a", "r"),
                rcv(fv(t_, "b", "t")),
                fv(t_, "j", "s")
        );
        Assertions.assertEquals(expectedL1TranslatedQueryValue, l1TranslatedQueryValue);

        /*
          let's construct a max match map (m3) using the translated value with the candidate value.
         */

        final var l1m3 = calculate(l1TranslatedQueryValue, p_v);
        Map<Value, Value> l1ExpectedMapping = Map.of(
                fv(t_, "a", "q"), fv(t_, "a", "q"),
                fv(t_, "a", "r"), fv(t_, "a", "r"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        Assertions.assertEquals(l1ExpectedMapping, l1m3.getMap());
        Assertions.assertEquals(expectedL1TranslatedQueryValue, l1m3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1m3.getCandidateResultValue());

        /*
             2nd level:
                  (p.2.0)                                          (p'.1.0)
                       [ p.0 < 42 ]                                      [ p_.0 < $Placeholder ]
                    |                                                 |
                  P |                                              p' |
                    |                                                 |
             (t.a.q, t.a.r, (t.b.t), t.j.s)      ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>
         */

        final var pAlias = CorrelationIdentifier.of("P");
        final var p_Alias = CorrelationIdentifier.of("P'");
        final var p = qov(pAlias, pv.getResultType());
        final var pPredicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(fv(p, 0), LiteralValue.ofScalar(42)));
        final var p_ = qov(p_Alias, p_v.getResultType());
        final var rv = rcv(fv(p, 2, 0));
        final var r_v = rcv(fv(p_, 1, 0));

        final var l2TranslationMap = pullUp(l1m3, pAlias, p_Alias);

        /*
           translation of (p.2.0) with correlation mapping of p -> p' (and the above m3) should
           give the following value (p'.1.0).
           Note that we have two instances of t'.b.t which are (p'.1.0, or p'.4), theoretically, the translationMap can
           choose either one, i.e. , however since pre-order traversal is used, the translation algorithm
           will always return the (p'.1.0) because it is the first node that matches the translated version of
           (p.2.0) in pre-order traversal.
         */

        final var l2TranslatedQueryValue = rv.translateCorrelations(l2TranslationMap, true);
        final var expectedL2TranslatedQueryValue = rcv(fv(p_, 1, 0));
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2TranslatedQueryValue);

        /*
           translation of the predicate p.0 < 42 should yield p'.0.0 < 42
         */
        final var l2TranslatedPredicate = pPredicate.translateCorrelations(l2TranslationMap, true);
        Assertions.assertEquals(new RelOpValue.LtFn().encapsulate(ImmutableList.of(fv(p_, 0, 0), LiteralValue.ofScalar(42))), l2TranslatedPredicate);

        final var l2ExpectedMapping = Map.of(rcv(fv(p_, 1, 0)), rcv(fv(p_, 1, 0)));
        final var l2m3 = calculate(l2TranslatedQueryValue, r_v);
        Assertions.assertEquals(l2ExpectedMapping, l2m3.getMap());
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2m3.getQueryResultValue());
        Assertions.assertEquals(r_v, l2m3.getCandidateResultValue());
    }

    @Test
    void maxMatchValueWithMatchableArithmeticOperationCase1() {
        // (t.a.q + t.a.r, (t.b.t), t.j.s)
        final var pv = rcv(
                add(fv(t, "a", "q"),
                        fv(t, "a", "r")),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );

        // ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), (t'.a.q + t'.a.r, t'.b.m), t'.j.q, t'.b.t, t'.b.m)
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

        final var l1TranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1M3 = calculate(pv.translateCorrelations(l1TranslationMap, true), p_v);

        final var expectedMapping = Map.of(
                add(fv(t_, "a", "q"), fv(t_, "a", "r")), add(fv(t_, "a", "q"), fv(t_, "a", "r")),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        final var expectedRewrittenQueryValue = rcv(
                add(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t")),
                fv(t_, "j", "s")
        );
        Assertions.assertEquals(expectedMapping, l1M3.getMap());
        Assertions.assertEquals(expectedRewrittenQueryValue, l1M3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1M3.getCandidateResultValue());
    }

    @Test
    void maxMatchValueWithMatchableArithmeticOperationCase2() {
        // (t.a.q + t.a.r + t.j.q, (t.b.t), t.j.s)
        final var pv = rcv(
                add(add(fv(t, "a", "q"),
                        fv(t, "a", "r")),
                    fv(t, "j", "q")),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );

        // ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.a.q + t'.a.r, t'.j.q, t'.b.t, t'.b.m)
        final var p_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                rcv(add(fv(t_, "a", "q"),
                                fv(t_, "a", "r"))),
                fv(t_, "j", "q"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        final var l1TranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1M3 = calculate(pv.translateCorrelations(l1TranslationMap, true), p_v);

        final var expectedMapping = Map.of(
                add(fv(t_, "a", "q"), fv(t_, "a", "r")), add(fv(t_, "a", "q"), fv(t_, "a", "r")),
                fv(t_, "j", "q"), fv(t_, "j", "q"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        final var expectedRewrittenQueryValue =  rcv(
                add(add(fv(t_, "a", "q"),
                                fv(t_, "a", "r")),
                        fv(t_, "j", "q")),
                rcv(fv(t_, "b", "t")),
                fv(t_, "j", "s")
        );
        Assertions.assertEquals(expectedMapping, l1M3.getMap());
        Assertions.assertEquals(expectedRewrittenQueryValue, l1M3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1M3.getCandidateResultValue());
    }

    @Test
    void maxMatchValueWithUnmatchableArithmeticOperationCase2() {
        // (t.a.q + t.a.r, (t.b.t), t.j.s)
        final var pv = rcv(
                add(fv(t, "a", "q"),
                        fv(t, "a", "r")),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );

        // ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), (t'.a.q + t'.a.r + t'.b.m), t'.j.q, t'.b.t, t'.b.m)
        final var p_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                rcv(add(add(fv(t_, "a", "q"),
                                fv(t_, "a", "r")),
                        fv(t_, "b", "m"))),
                fv(t_, "j", "q"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        final var l1TranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1M3 = calculate(pv.translateCorrelations(l1TranslationMap, true), p_v);

        final var expectedMapping = Map.of(
                fv(t_, "a", "q"), fv(t_, "a", "q"),
                fv(t_, "a", "r"), fv(t_, "a", "r"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        final var expectedRewrittenQueryValue = rcv(
                add(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t")),
                fv(t_, "j", "s")
        );
        Assertions.assertEquals(expectedMapping, l1M3.getMap());
        Assertions.assertEquals(expectedRewrittenQueryValue, l1M3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1M3.getCandidateResultValue());
    }

    @Test
    public void maxMatchValueWithMatchableArithmeticOperationAndOtherConstantCorrelations() {
        /*
             1st level:
             (t.a.q + s, t.a.r, (t.b.t), t.j.s)      ((t'.a.q + s', t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>
         */

        final var pv = rcv(
                add(fv(t, "a", "q"), s),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );
        final var p_v = rcv(
                rcv(add(fv(t_, "a", "q"), s_),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                fv(t_, "j", "q"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        /*
           translation of (t.a.q + s, t.a.r, (t.b.t), t.j.s) with correlation mapping of t -> t', s -> s' (and no m3) should merely
           replace t with t', i.e. the result should be (t'.a.q + s, t'.a.r, (t'.b.t), t'.j.s)
         */

        final var l1TranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1TranslatedQueryValue = pv.translateCorrelations(l1TranslationMap, true);
        final var expectedL1TranslatedQueryValue = rcv(
                add(fv(t_, "a", "q"), s),
                fv(t_, "a", "r"),
                rcv(fv(t_, "b", "t")),
                fv(t_, "j", "s")
        );
        Assertions.assertEquals(expectedL1TranslatedQueryValue, l1TranslatedQueryValue);

        /*
          let's construct a max match map (m3) using the translated value with the candidate value.
         */

        final var l1m3 =
                MaxMatchMap.calculate(l1TranslatedQueryValue, p_v, ImmutableSet.of(),
                        ValueEquivalence.fromAliasMap(AliasMap.ofAliases(sAlias, s_Alias)));

        Map<Value, Value> l1ExpectedMapping = Map.of(
                add(fv(t_, "a", "q"), s),  add(fv(t_, "a", "q"), s_),
                fv(t_, "a", "r"), fv(t_, "a", "r"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        Assertions.assertEquals(l1ExpectedMapping, l1m3.getMap());
        Assertions.assertEquals(expectedL1TranslatedQueryValue, l1m3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1m3.getCandidateResultValue());

        /*
             2nd level:
                  (p.2.0)                                          (p'.1.0)
                       [ p.0 < 42 ]                                 [ p_.0 < $Placeholder ]
                    |                                                 |
                  P |                                              p' |
                    |                                                 |
             (t.a.q + s, t.a.r, (t.b.t), t.j.s)      ((t'.a.q + s', t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>
         */

        final var pAlias = CorrelationIdentifier.of("P");
        final var p_Alias = CorrelationIdentifier.of("P'");
        final var p = qov(pAlias, pv.getResultType());
        final var pPredicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(fv(p, 0), LiteralValue.ofScalar(42)));
        final var p_ = qov(p_Alias, p_v.getResultType());
        final var rv = rcv(fv(p, 2, 0));
        final var r_v = rcv(fv(p_, 1, 0));

        final var l2TranslationMap = pullUp(l1m3, pAlias, p_Alias);

        /*
           translation of (p.2.0) with correlation mapping of p -> p' (and the above m3) should
           give the following value (p'.1.0).
           Note that we have two instances of t'.b.t which are (p'.1.0, or p'.4), theoretically, the translationMap can
           choose either one, i.e. , however since pre-order traversal is used, the translation algorithm
           will always return the (p'.1.0) because it is the first node that matches the translated version of
           (p.2.0) in pre-order traversal.
         */

        final var l2TranslatedQueryValue = rv.translateCorrelations(l2TranslationMap, true);
        final var expectedL2TranslatedQueryValue = rcv(fv(p_, 1, 0));
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2TranslatedQueryValue);

        /*
           translation of the predicate p.0 < 42 should yield p'.0.0 < 42
         */
        final var l2TranslatedPredicate = pPredicate.translateCorrelations(l2TranslationMap, true);
        Assertions.assertEquals(new RelOpValue.LtFn().encapsulate(ImmutableList.of(fv(p_, 0, 0), LiteralValue.ofScalar(42))), l2TranslatedPredicate);

        final var l2ExpectedMapping = Map.of(rcv(fv(p_, 1, 0)), rcv(fv(p_, 1, 0)));
        final var l2m3 = calculate(l2TranslatedQueryValue, r_v);
        Assertions.assertEquals(l2ExpectedMapping, l2m3.getMap());
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2m3.getQueryResultValue());
        Assertions.assertEquals(r_v, l2m3.getCandidateResultValue());
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record getMType() {
        return r(
                f("m1", r("m11", "m12")),
                f("m2", r("m21", "m22")),
                f("m3", r("m31", "m32"))
        );
    }

    @Nonnull
    private Type.Record getNType() {
        return r(
                f("n1", r("n11", "n12")),
                f("n2", r("n21", "n22")),
                f("n3", r("n31", "n32"))
        );
    }

    @Test
    public void maxMatchValueWithCompositionOfTranslationMaps() {
        /*
             1st level:
             (t.a.q, t.a.r, (t.b.t), t.j.s)      ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>

            ((m.m1.m11), m.m2.m21)               (m'.m3.m31, (m'.m2.m21), m'.m1.m11)
                   |                                                 |
                M  |                                              M' |
                   |                                                 |
                  <M>                                               <M'>

            (n.n2.n21, (n.n1.n12, n.n3.n32))     ((n'.n3.n32), n'.n1.n12, (n'.n3.n31, n'.n2.n22, n'.n2.n21)
                   |                                                 |
                 N |                                              N' |
                   |                                                 |
                  <N>                                               <N'>
         */

        final var tv = rcv(
                fv(t, "a", "q"),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );
        final var t_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                fv(t_, "j", "q"),
                fv(t_, "b", "t")
        );

        final var mv = rcv(
                rcv(fv(m, "m1", "m11")),
                fv(m, "m2", "m21")
        );

        final var m_v = rcv(
                fv(m_, "m3", "m31"),
                rcv(fv(m_, "m2", "m21")),
                fv(m_, "m1", "m11")
        );

        final var nv = rcv(
                fv(n, "n2", "n21"),
                rcv(fv(n, "n1", "n12"), fv(n, "n3", "n32"))
        );

        final var n_v = rcv(
                rcv(fv(n_, "n3", "n32")),
                fv(n_, "n1", "n12"),
                rcv(fv(n_, "n3", "n31"), fv(n_, "n2", "n22"), fv(n_, "n2", "n21"))
        );

        /*
           translation of (t.a.q, t.a.r, (t.b.t), t.j.s) with correlation mapping of t -> t' (and no m3) should merely
           replace t with t', i.e. the result should be (t'.a.q, t'.a.r, (t'.b.t), t'.j.s), same applies for translation
           of ((m.m1.m11), m.m2.m21) and (n.n2.n21, (n.n1.n12, n.n3.n32)); i.e. m -> m', resp. n -> n'.
         */

        final var l1TranslationMapTValue = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1TranslatedQueryTValue = tv.translateCorrelations(l1TranslationMapTValue, true);
        final var expectedL1TranslatedQueryTValue = rcv(
                fv(t_, "a", "q"),
                fv(t_, "a", "r"),
                rcv(fv(t_, "b", "t")),
                fv(t_, "j", "s")
        );
        Assertions.assertEquals(expectedL1TranslatedQueryTValue, l1TranslatedQueryTValue);

        final var l1TranslationMapMValue = TranslationMap.ofAliases(mAlias, m_Alias);
        final var l1TranslatedQueryMValue = mv.translateCorrelations(l1TranslationMapMValue, true);
        final var expectedL1TranslatedQueryMValue = rcv(
                rcv(fv(m_, "m1", "m11")),
                fv(m_, "m2", "m21")
        );
        Assertions.assertEquals(expectedL1TranslatedQueryMValue, l1TranslatedQueryMValue);

        final var l1TranslationMapNValue = TranslationMap.ofAliases(nAlias, n_Alias);
        final var l1TranslatedQueryNValue = nv.translateCorrelations(l1TranslationMapNValue, true);
        final var expectedL1TranslatedQueryNValue = rcv(
                fv(n_, "n2", "n21"),
                rcv(fv(n_, "n1", "n12"), fv(n_, "n3", "n32"))
        );
        Assertions.assertEquals(expectedL1TranslatedQueryNValue, l1TranslatedQueryNValue);

        /*
          let's construct a max match map (m3) using the translated value with the candidate value, for tv, mv, and nv.
         */

        final var l1m3ForTValue = calculate(l1TranslatedQueryTValue, t_v);

        Map<Value, Value> l1ExpectedMappingForTValue = Map.of(
                fv(t_, "a", "q"), fv(t_, "a", "q"),
                fv(t_, "a", "r"), fv(t_, "a", "r"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        Assertions.assertEquals(l1ExpectedMappingForTValue, l1m3ForTValue.getMap());
        Assertions.assertEquals(expectedL1TranslatedQueryTValue, l1m3ForTValue.getQueryResultValue());
        Assertions.assertEquals(t_v, l1m3ForTValue.getCandidateResultValue());

        final var l1m3ForMValue = calculate(l1TranslatedQueryMValue, m_v);

        Map<Value, Value> l1ExpectedMappingForMValue = Map.of(
                fv(m_, "m1", "m11"), fv(m_, "m1", "m11"),
                fv(m_, "m2", "m21"), fv(m_, "m2", "m21"));
        Assertions.assertEquals(l1ExpectedMappingForMValue, l1m3ForMValue.getMap());
        Assertions.assertEquals(expectedL1TranslatedQueryMValue, l1m3ForMValue.getQueryResultValue());
        Assertions.assertEquals(m_v, l1m3ForMValue.getCandidateResultValue());

        final var l1m3ForNValue = calculate(l1TranslatedQueryNValue, n_v);

        Map<Value, Value> l1ExpectedMappingForNValue = Map.of(
                fv(n_, "n2", "n21"), fv(n_, "n2", "n21"),
                fv(n_, "n1", "n12"), fv(n_, "n1", "n12"),
                fv(n_, "n3", "n32"), fv(n_, "n3", "n32"));
        Assertions.assertEquals(l1ExpectedMappingForNValue, l1m3ForNValue.getMap());
        Assertions.assertEquals(expectedL1TranslatedQueryNValue, l1m3ForNValue.getQueryResultValue());
        Assertions.assertEquals(n_v, l1m3ForNValue.getCandidateResultValue());

        // translate a complex join condition, each quantifier in the join condition is assumed to match a corresponding
        // quantifier in a non-joined index candidate.

        /*
             2nd level:
                  (p.2.0)
                       [ p.0 + q.0.0 < r.0 - r.1.0 ]
                    |   |           | R
                    |   | Q         --------------------------------------------------
                  P |   -------------------------------                              |
                    |                                 |                              |
             (t.a.q, t.a.r, (t.b.t), t.j.s)    ((m.m1.m11), m.m2.m21)       (n.n2.n21, (n.n1.n12, n.n3.n32))
                    |                                |                                  |
                  T |                              M |                                N |
                    |                                |                                  |
                   <T>                              <M>                                <N>

             Candidates at 2nd level:
             on T':
             ======
                       <resultValue>
                           [ p'.0.0 < $Placeholder ]
                        |
                     P' |
                        |
               ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                        |
                     T' |
                        |
                       <T'>

             on M':
             ======
                     <resultValue>
                           [ q'.2.0 < $Placeholder ]
                        |
                     Q' |
                        |
                (m'.m3.m31, (m'.m2.m21), m'.m1.m11)
                                    |
                                 M' |
                                    |
                                   <M'>

             on N':
             ======
                    <resultValue>
                           [ r'.3 < $Placeholder, r'.1 < $Placeholder ]
                        |
                     R' |
                        |
                 ((n'.n3.n32), n'.n1.n12, (n'.n3.n31, n'.n2.n22, n'.n2.n21)
                                |
                             N' |
                                |
                               <N'>
         */

        final var pAlias = CorrelationIdentifier.of("P");
        final var p_Alias = CorrelationIdentifier.of("P'");
        final var p = qov(pAlias, tv.getResultType());
        final var qAlias = CorrelationIdentifier.of("Q");
        final var q_Alias = CorrelationIdentifier.of("Q'");
        final var q = qov(qAlias, mv.getResultType());
        final var rAlias = CorrelationIdentifier.of("R");
        final var r_Alias = CorrelationIdentifier.of("R'");
        final var r = qov(rAlias, nv.getResultType());

        // p.0 + q.0.0 < n.0 - n.1.0
        final var predicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(add(fv(p, 0), fv(q, 0, 0)), add(fv(r, 0), fv(r, 1, 0))));


        final var l2TranslationMapForPValue = pullUp(l1m3ForTValue, pAlias, p_Alias);
        final var l2TranslationMapForQValue = pullUp(l1m3ForMValue, qAlias, q_Alias);
        final var l2TranslationMapForRValue = pullUp(l1m3ForNValue, rAlias, r_Alias);
        final var compositeTranslationMap = TranslationMap.compose(ImmutableList.of(l2TranslationMapForPValue, l2TranslationMapForQValue, l2TranslationMapForRValue));
        final var translatedPredicate = predicate.translateCorrelations(compositeTranslationMap, true);

        final var p_ = qov(p_Alias, t_v.getResultType());
        final var q_ = qov(q_Alias, m_v.getResultType());
        final var r_ = qov(r_Alias, n_v.getResultType());

        final var expectedTranslatedPredicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(add(fv(p_, 0, 0), fv(q_, 2)), add(fv(r_, 2, 2), fv(r_, 1))));

        Assertions.assertEquals(expectedTranslatedPredicate, translatedPredicate);
    }

    @Test
    void validTranslationMapCompositions() {
        final var tv = rcv(
                fv(t, "a", "q"),
                add(s, fv(t, "a", "r")),
                add(s, u)
        );

        // s R s ≡ s R s R s .... R s, not allowed.
        {
            final var translationMap = TranslationMap.ofAliases(tAlias, t_Alias);
            Assertions.assertThrows(VerifyException.class,
                    () -> TranslationMap.compose(ImmutableList.of(translationMap, translationMap, translationMap, translationMap)));
        }

        // t R s ≡ s R t
        {
            // t R s
            final var tTranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
            final var sTranslationMap = TranslationMap.ofAliases(sAlias, s_Alias);
            final var compositeTranslationMap = TranslationMap.compose(ImmutableList.of(tTranslationMap, sTranslationMap));
            final var translatedValue = tv.translateCorrelations(compositeTranslationMap, true);
            final var expectedTranslatedValue = rcv(
                    fv(t_, "a", "q"),
                    add(s_, fv(t_, "a", "r")),
                    add(s_, u)
            );
            Assertions.assertEquals(expectedTranslatedValue, translatedValue);

            // s R t
            final var symmetricTranslationMap = TranslationMap.compose(ImmutableList.of(sTranslationMap, tTranslationMap));
            final var identicalTranslatedValue = tv.translateCorrelations(symmetricTranslationMap, true);
            Assertions.assertEquals(identicalTranslatedValue, expectedTranslatedValue);
        }

        // (t R s) R u ≡ t R (s R u)
        {
            final var tTranslationMap = TranslationMap.ofAliases(tAlias, t_Alias);
            final var sTranslationMap = TranslationMap.ofAliases(sAlias, s_Alias);
            final var uTranslationMap = TranslationMap.ofAliases(uAlias, u_Alias);
            final var tsTranslationMap = TranslationMap.compose(ImmutableList.of(tTranslationMap, sTranslationMap));

            // (t R s) R u
            final var ts_uTranslationMap = TranslationMap.compose(ImmutableList.of(tsTranslationMap, uTranslationMap));
            final var translatedValue = tv.translateCorrelations(ts_uTranslationMap, true);
            final var expectedTranslatedValue = rcv(
                    fv(t_, "a", "q"),
                    add(s_, fv(t_, "a", "r")),
                    add(s_, u_)
            );
            Assertions.assertEquals(expectedTranslatedValue, translatedValue);

            // t R (s R u)
            final var suTranslationMap = TranslationMap.compose(ImmutableList.of(sTranslationMap, uTranslationMap));
            final var t_suTranslationMap = TranslationMap.compose(ImmutableList.of(suTranslationMap, tTranslationMap));
            final var identicalTranslatedValue = tv.translateCorrelations(t_suTranslationMap, true);
            Assertions.assertEquals(identicalTranslatedValue, translatedValue);
        }
    }

    @Test
    public void maxMatchDifferentCompositions() {
        final var tv = rcv(
                fv(t, "a", "q"),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );
        final var t_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                fv(t_, "j", "q"),
                fv(t_, "b", "t")
        );

        final var mv = rcv(
                rcv(fv(m, "m1", "m11")),
                fv(m, "m2", "m21")
        );

        final var m_v = rcv(
                fv(m_, "m3", "m31"),
                rcv(fv(m_, "m2", "m21")),
                fv(m_, "m1", "m11")
        );

        final var nv = rcv(
                fv(n, "n2", "n21"),
                rcv(fv(n, "n1", "n12"), fv(n, "n3", "n32"))
        );

        final var n_v = rcv(
                rcv(fv(n_, "n3", "n32")),
                fv(n_, "n1", "n12"),
                rcv(fv(n_, "n3", "n31"), fv(n_, "n2", "n22"), fv(n_, "n2", "n21"))
        );

        /*
             1st level:
             (t.a.q, t.a.r, (t.b.t), t.j.s)      ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>

            ((m.m1.m11), m.m2.m21)               (m'.m3.m31, (m'.m2.m21), m'.m1.m11)
                   |                                                 |
                M  |                                              M' |
                   |                                                 |
                  <M>                                               <M'>

            (n.n2.n21, (n.n1.n12, n.n3.n32))     ((n'.n3.n32), n'.n1.n12, (n'.n3.n31, n'.n2.n22, n'.n2.n21)
                   |                                                 |
                 N |                                              N' |
                   |                                                 |
                  <N>                                               <N'>
         */

        /*
           translation of (t.a.q, t.a.r, (t.b.t), t.j.s) with correlation mapping of t -> t' (and no m3) should merely
           replace t with t', i.e. the result should be (t'.a.q, t'.a.r, (t'.b.t), t'.j.s), same applies for translation
           of ((m.m1.m11), m.m2.m21) and (n.n2.n21, (n.n1.n12, n.n3.n32)); i.e. m -> m', resp. n -> n'.
         */

        final var l1TranslationMapTValue = TranslationMap.ofAliases(tAlias, t_Alias);
        final var l1TranslationMapMValue = TranslationMap.ofAliases(mAlias, m_Alias);
        final var l1TranslationMapNValue = TranslationMap.ofAliases(nAlias, n_Alias);

        final var l1TranslatedQueryTValue = tv.translateCorrelations(l1TranslationMapTValue, true);
        final var l1TranslatedQueryMValue = mv.translateCorrelations(l1TranslationMapMValue, true);
        final var l1TranslatedQueryNValue = nv.translateCorrelations(l1TranslationMapNValue, true);

        //
        //  Let's construct a max match map (m3) using the translated value with the candidate value,
        //  for tv, mv, and nv.
        //
        final var l1m3ForTValue = calculate(l1TranslatedQueryTValue, t_v);
        final var l1m3ForMValue = calculate(l1TranslatedQueryMValue, m_v);
        final var l1m3ForNValue = calculate(l1TranslatedQueryNValue, n_v);

        // translate a complex join condition, each quantifier in the join condition is assumed to match a corresponding
        // quantifier in a non-joined index candidate.

        /*
             2nd level:
                  (p.2.0)
                       [ p.0 + q.0.0 < r.0 + s + u ]
                    |   |           |     |    |
                    |   |           |     |    ---------------------------------------------------------------------------------
                    |   |           | R   ---------------------------------------------------------------------------------    |
                    |   | Q         --------------------------------------------------                                    |    |
                  P |   -------------------------------                              |                                   <S>  <U>
                    |                                 |                              |
             (t.a.q, t.a.r, (t.b.t), t.j.s)    ((m.m1.m11), m.m2.m21)       (n.n2.n21, (n.n1.n12, n.n3.n32))
                    |                                |                                  |
                  T |                              M |                                N |
                    |                                |                                  |
                   <T>                              <M>                                <N>

             Candidates at 2nd level:
             on T':
             ======
                       <resultValue>
                           [ p'.0.0 < $Placeholder ]
                        |
                     P' |
                        |
               ((t'.a.q, t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                        |
                     T' |
                        |
                       <T'>

             on M':
             ======
                     <resultValue>
                           [ q'.2.0 < $Placeholder ]
                        |
                     Q' |
                        |
                (m'.m3.m31, (m'.m2.m21), m'.m1.m11)
                                    |
                                 M' |
                                    |
                                   <M'>

             on N':
             ======
                    <resultValue>
                           [ r'.3 < $Placeholder, r'.1 < $Placeholder ]
                        |
                     R' |
                        |
                 ((n'.n3.n32), n'.n1.n12, (n'.n3.n31, n'.n2.n22, n'.n2.n21)
                                |
                             N' |
                                |
                               <N'>
             Having (flat) candidate correlations corresponding to SimpleTranslationMaps
             S':
                   QOV(int)
             U':
                   QOV(int)
         */

        final var pAlias = CorrelationIdentifier.of("P");
        final var p_Alias = CorrelationIdentifier.of("P'");
        final var p = qov(pAlias, tv.getResultType());
        final var qAlias = CorrelationIdentifier.of("Q");
        final var q_Alias = CorrelationIdentifier.of("Q'");
        final var q = qov(qAlias, mv.getResultType());
        final var rAlias = CorrelationIdentifier.of("R");
        final var r_Alias = CorrelationIdentifier.of("R'");
        final var r = qov(rAlias, nv.getResultType());

        // p.0 + q.0.0 < r.0 + s + u
        final var predicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(add(fv(p, 0), fv(q, 0, 0)), add(add(fv(r, 0), s), u)));

        final var translationMaps = new ArrayList<TranslationMap>();
        translationMaps.add(pullUp(l1m3ForTValue, pAlias, p_Alias));
        translationMaps.add(pullUp(l1m3ForMValue, qAlias, q_Alias));
        translationMaps.add(pullUp(l1m3ForNValue, rAlias, r_Alias));
        translationMaps.add(TranslationMap.ofAliases(sAlias, s_Alias));
        translationMaps.add(TranslationMap.ofAliases(uAlias, u_Alias));

        final var p_ = qov(p_Alias, t_v.getResultType());
        final var q_ = qov(q_Alias, m_v.getResultType());
        final var r_ = qov(r_Alias, n_v.getResultType());

        final var expectedTranslatedPredicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(add(fv(p_, 0, 0), fv(q_, 2)), add(add(fv(r_, 2, 2), s_), u_)));

        final var random = new Random(42);
        for (int i = 0; i < 10; i++) {
            Collections.shuffle(translationMaps, random);
            final var compositeTranslationMap = TranslationMap.compose(translationMaps);
            final var translatedPredicate = predicate.translateCorrelations(compositeTranslationMap, true);
            Assertions.assertEquals(expectedTranslatedPredicate, translatedPredicate);
        }
    }

    /**
     * Test to establish that simple a simple QueriedValue() can be matched to another QueriedValue().
     */
    @Test
    public void maxMatchValueSimpleQueriedValues() {
        final var pv = new QueriedValue(getTType(), ImmutableList.of("T"));
        final var p_v = new QueriedValue(getTType(), ImmutableList.of("T"));

        /*
           translation of (
           QueriedValue()) with empty correlation mapping (and no m3) should not cause any translation
         */

        final var l1TranslationMap = TranslationMap.empty();
        final var l1TranslatedQueryValue = pv.translateCorrelations(l1TranslationMap, true);
        Assertions.assertEquals(pv, l1TranslatedQueryValue);

        /*
          let's construct a max match map (m3) using the translated value with the candidate value.
         */
        final var l1m3 = calculate(l1TranslatedQueryValue, p_v);

        Map<Value, Value> l1ExpectedMapping = Map.of(pv,  p_v);
        Assertions.assertEquals(l1ExpectedMapping, l1m3.getMap());
        Assertions.assertEquals(pv, l1m3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1m3.getCandidateResultValue());

        final var pAlias = CorrelationIdentifier.of("P");
        final var p_Alias = CorrelationIdentifier.of("P'");
        final var p = qov(pAlias, pv.getResultType());
        final var pPredicate = (Value)new RelOpValue.LtFn().encapsulate(ImmutableList.of(fv(p, 0, 0),
                LiteralValue.ofScalar(42)));
        final var p_ = qov(p_Alias, p_v.getResultType());

        final var l2TranslationMap = pullUp(l1m3, pAlias, p_Alias);

        /*
           translation of the predicate p.0.0 < 42 should yield p'.0.0 < 42
         */
        final var l2TranslatedPredicate = pPredicate.translateCorrelations(l2TranslationMap, true);
        Assertions.assertEquals(new RelOpValue.LtFn().encapsulate(ImmutableList.of(fv(p_, 0, 0),
                LiteralValue.ofScalar(42))), l2TranslatedPredicate);
    }

    /**
     * Test to establish that simple QOV(T') (fields a, b, j) can be matched to a RCV(T'.a, T'.b, T'.j).
     */
    @Test
    public void maxMatchQovUsingExpandedSimpleQov() {
        final var p_v = rcv(true,
                fv(t_, "a"), "a",
                fv(t_, "b"), "b",
                fv(t_, "j"), "j");

        final var m3 = calculate(t_, p_v);

        final var computedMap = m3.getMap();
        final var expectedMap = ImmutableMap.of(p_v, p_v);
        Assertions.assertEquals(expectedMap, computedMap);
    }

    /**
     * Test to establish that simple QOV(T') can be matched to a deconstructed RCV(T'.a, T'.b, T'.j).
     */
    @Test
    public void maxMatchQovUsingExpandedQovReorderedFields() {
        final var p_v = rcv(fv(t_, "b"), fv(t_, "a"), fv(t_, "j"));

        final var m3 = calculate(t_, p_v);

        final var computedMap = m3.getMap();
        final var expectedMap =
                ImmutableMap.of(fv(t_, "a"), fv(t_, "a"),
                        fv(t_, "b"), fv(t_, "b"),
                        fv(t_, "j"), fv(t_, "j"));
        Assertions.assertEquals(expectedMap, computedMap);
    }

    /**
     * Test to establish that simple QOV(T') can be matched to an almost completely deconstructed
     * RCV(RCV(T'.a.q, T'.a.r), T'.b, T'.j).
     */
    @Test
    public void maxMatchQovUsingExpandedQovComplex1() {
        final var p_v =
                rcv(true,
                        rcv(true, fv(t_, "a", "q"), "q",
                                fv(t_, "a", "r"), "r"), "a",
                        fv(t_, "b"), "b",
                        fv(t_, "j"), "j");

        final var m3 = calculate(t_, p_v);

        final var computedMap = m3.getMap();
        final var expectedMap = ImmutableMap.of(p_v, p_v);
        Assertions.assertEquals(expectedMap, computedMap);
    }

    /**
     * Test to establish that simple QOV(T') can be matched to an almost completely deconstructed
     * RCV(RCV(T'.a.q, T'.a.r), T'.b, T'.j).
     */
    @Test
    public void maxMatchFvUsingExpandedQovComplex1() {
        final var pv =
                rcv(fv(t_, "a", "q"));
        final var p_v =
                rcv(fv(t_, "a"));

        final var m3 = calculate(pv, p_v);

        final var computedMap = m3.getMap();
        final var expectedMap = ImmutableMap.of(fv(t_, "a"), fv(t_, "a"));
        Assertions.assertEquals(expectedMap, computedMap);
    }

    @Nonnull
    private static MaxMatchMap calculate(@Nonnull final Value queryResultValue, @Nonnull final Value candidateResultValue) {
        return MaxMatchMap.calculate(queryResultValue, candidateResultValue, ImmutableSet.of());
    }

    @Nonnull
    private static TranslationMap pullUp(@Nonnull MaxMatchMap maxMatchMap,
                                         @Nonnull final CorrelationIdentifier queryAlias,
                                         @Nonnull final CorrelationIdentifier candidateAlias) {
        final var translatedQueryValueOptional = maxMatchMap.translateQueryValueMaybe(candidateAlias);
        final var translationMapOptional = translatedQueryValueOptional.map(translatedQueryValue ->
                TranslationMap.builder()
                        .when(queryAlias).then((src, quantifiedValue) -> translatedQueryValue)
                        .build());
        Assertions.assertTrue(translationMapOptional.isPresent());
        return translationMapOptional.get();
    }
}
