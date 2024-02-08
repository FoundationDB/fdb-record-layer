/*
 * ValueMaxMatch2Test.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.Translator;
import com.google.common.base.Verify;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests for value mapping.
 */
public class ValueTranslationTest {

    @SuppressWarnings("checkstyle:MemberName")
    @Nonnull
    private final CorrelationIdentifier P = CorrelationIdentifier.of("P");

    @SuppressWarnings("checkstyle:MemberName")
    @Nonnull
    private final CorrelationIdentifier Q = CorrelationIdentifier.of("Q");

    @Nonnull
    private Type.Record getRecordType() {
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

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private QuantifiedObjectValue qov(@Nonnull final String name) {
        return qov(name, getRecordType());
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private QuantifiedObjectValue qov(@Nonnull final String name, @Nonnull final Type type) {
        return qov(CorrelationIdentifier.of(name), type);
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private QuantifiedObjectValue qov(@Nonnull final CorrelationIdentifier name, @Nonnull final Type type) {
        return QuantifiedObjectValue.of(name, type);
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record r(String... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .map(this::f)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private Type.Record r(Type.Record.Field... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .collect(Collectors.toList()));
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
        return RecordConstructorValue.ofUnnamed(Arrays.stream(values).collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    @Nonnull
    private FieldValue fv(@Nonnull final Value base, String... name) {
        return fvInternal(base, name.length - 1, name);
    }

    @Nonnull
    private FieldValue fv(String... name) {
        return fvInternal(ObjectValue.of(P, getRecordType()), name.length - 1, name);
    }

    @Nonnull
    private FieldValue fv(@Nonnull final Value base, Integer... indexes) {
        return fvInternal(base, indexes.length - 1, indexes);
    }

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
            T has the following type:
                (a, b, j) | type(a) = (q, r), type(b) = (t, m), type(j) = (s,q)
         */

        final var tAlias = CorrelationIdentifier.of("T");
        final var t_Alias = CorrelationIdentifier.of("T'");
        final var t = qov(tAlias, getTType());
        final var t_ = qov(t_Alias, getTType());

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

        final var l1Translator = Translator.builder().ofCorrelations(tAlias, t_Alias).build();
        final var l1TranslatedQueryValue = l1Translator.translate(pv);
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

        final var l1m3 = l1Translator.calculateMaxMatches(l1TranslatedQueryValue, p_v);

        Map<Value, Value> l1ExpectedMapping = Map.of(
                fv(t_, "a", "q"), fv(t_, "a", "q"),
                fv(t_, "a", "r"), fv(t_, "a", "r"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        Assertions.assertEquals(l1ExpectedMapping, l1m3.getMapping());
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
        final var pPredicate = (Value)new RelOpValue.LtFn().encapsulate(List.of(fv(p, 0), LiteralValue.ofScalar(42)));
        final var p_ = qov(p_Alias, p_v.getResultType());
        final var rv = rcv(fv(p, 2, 0));
        final var r_v = rcv(fv(p_, 1, 0));

        final var l2Translator = Translator.builder().ofCorrelations(pAlias, p_Alias).using(l1m3).build();

        /*
           translation of (p.2.0) with correlation mapping of p -> p' (and the above m3) should
           give the following value (p'.1.0).
           Note that we have two instances of t'.b.t which are (p'.1.0, or p'.4), theoretically, the translator can
           choose either one, i.e. , however since pre-order traversal is used, the translation algorithm
           will always return the (p'.1.0) because it is the first node that matches the translated version of
           (p.2.0) in pre-order traversal.
         */

        final var l2TranslatedQueryValue = l2Translator.translate(rv);
        final var expectedL2TranslatedQueryValue = rcv(fv(p_, 1, 0));
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2TranslatedQueryValue);

        /*
           translation of the predicate p.0 < 42 should yield p'.0.0 < 42
         */
        final var l2TranslatedPredicate = l2Translator.translate(pPredicate);
        Assertions.assertEquals(new RelOpValue.LtFn().encapsulate(List.of(fv(p_, 0, 0), LiteralValue.ofScalar(42))), l2TranslatedPredicate);

        final var l2ExpectedMapping = Map.of(rcv(fv(p_, 1, 0)), rcv(fv(p_, 1, 0)));
        final var l2m3 = l2Translator.calculateMaxMatches(l2TranslatedQueryValue, r_v);
        Assertions.assertEquals(l2ExpectedMapping, l2m3.getMapping());
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2m3.getQueryResultValue());
        Assertions.assertEquals(r_v, l2m3.getCandidateResultValue());
    }

    @Test
    void maxMatchValueWithMatchableArithmeticOperationCase1() {
        final var tAlias = CorrelationIdentifier.of("T");
        final var t_Alias = CorrelationIdentifier.of("T'");
        final var t = qov(tAlias, getTType());
        final var t_ = qov(t_Alias, getTType());

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

        final var l1Translator = Translator.builder().ofCorrelations(tAlias, t_Alias).build();
        final var l1M3 = l1Translator.calculateMaxMatches(l1Translator.translate(pv), p_v);

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
        Assertions.assertEquals(expectedMapping, l1M3.getMapping());
        Assertions.assertEquals(expectedRewrittenQueryValue, l1M3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1M3.getCandidateResultValue());
    }

    @Test
    void maxMatchValueWithMatchableArithmeticOperationCase2() {
        final var tAlias = CorrelationIdentifier.of("T");
        final var t_Alias = CorrelationIdentifier.of("T'");
        final var t = qov(tAlias, getTType());
        final var t_ = qov(t_Alias, getTType());

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

        final var l1Translator = Translator.builder().ofCorrelations(tAlias, t_Alias).build();
        final var l1M3 = l1Translator.calculateMaxMatches(l1Translator.translate(pv), p_v);

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
        Assertions.assertEquals(expectedMapping, l1M3.getMapping());
        Assertions.assertEquals(expectedRewrittenQueryValue, l1M3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1M3.getCandidateResultValue());
    }

    @Test
    void maxMatchValueWithUnmatchableArithmeticOperationCase2() {
        final var tAlias = CorrelationIdentifier.of("T");
        final var t_Alias = CorrelationIdentifier.of("T'");
        final var t = qov(tAlias, getTType());
        final var t_ = qov(t_Alias, getTType());

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

        final var l1Translator = Translator.builder().ofCorrelations(tAlias, t_Alias).build();
        final var l1M3 = l1Translator.calculateMaxMatches(l1Translator.translate(pv), p_v);

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
        Assertions.assertEquals(expectedMapping, l1M3.getMapping());
        Assertions.assertEquals(expectedRewrittenQueryValue, l1M3.getQueryResultValue());
        Assertions.assertEquals(p_v, l1M3.getCandidateResultValue());
    }

    @Test
    public void maxMatchValueWithMatchableArithmeticOperationAndOtherConstantCorrelations() throws Exception {
        /*
            T has the following type:
                (a, b, j) | type(a) = (q, r), type(b) = (t, m), type(j) = (s,q)
         */

        final var tAlias = CorrelationIdentifier.of("T");
        final var t_Alias = CorrelationIdentifier.of("T'");
        final var t = qov(tAlias, getTType());
        final var t_ = qov(t_Alias, getTType());

        /*
             1st level:
             (t.a.q + s, t.a.r, (t.b.t), t.j.s)      ((t'.a.q + s', t'.a.r), (t'.b.t, t'.b.m), t'.j.s, t'.j.q, t'.b.t, t'.b.m)
                    |                                                 |
                  T |                                              T' |
                    |                                                 |
                   <T>                                               <T'>
         */


        final var sAlias = CorrelationIdentifier.of("S");
        final var s = qov(sAlias, getSType());
        final var s_Alias = CorrelationIdentifier.of("S'");
        final var s_ = qov(s_Alias, getSType());

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

        final var l1Translator = Translator.builder().ofCorrelations(tAlias, t_Alias).withConstantAliaMap(AliasMap.of(sAlias, s_Alias)).build();
        final var l1TranslatedQueryValue = l1Translator.translate(pv);
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

        final var l1m3 = l1Translator.calculateMaxMatches(l1TranslatedQueryValue, p_v);

        Map<Value, Value> l1ExpectedMapping = Map.of(
                add(fv(t_, "a", "q"), s),  add(fv(t_, "a", "q"), s_),
                fv(t_, "a", "r"), fv(t_, "a", "r"),
                fv(t_, "b", "t"), fv(t_, "b", "t"),
                fv(t_, "j", "s"), fv(t_, "j", "s"));
        Assertions.assertEquals(l1ExpectedMapping, l1m3.getMapping());
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
        final var pPredicate = (Value)new RelOpValue.LtFn().encapsulate(List.of(fv(p, 0), LiteralValue.ofScalar(42)));
        final var p_ = qov(p_Alias, p_v.getResultType());
        final var rv = rcv(fv(p, 2, 0));
        final var r_v = rcv(fv(p_, 1, 0));

        final var l2Translator = Translator.builder().ofCorrelations(pAlias, p_Alias).using(l1m3).build();

        /*
           translation of (p.2.0) with correlation mapping of p -> p' (and the above m3) should
           give the following value (p'.1.0).
           Note that we have two instances of t'.b.t which are (p'.1.0, or p'.4), theoretically, the translator can
           choose either one, i.e. , however since pre-order traversal is used, the translation algorithm
           will always return the (p'.1.0) because it is the first node that matches the translated version of
           (p.2.0) in pre-order traversal.
         */

        final var l2TranslatedQueryValue = l2Translator.translate(rv);
        final var expectedL2TranslatedQueryValue = rcv(fv(p_, 1, 0));
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2TranslatedQueryValue);

        /*
           translation of the predicate p.0 < 42 should yield p'.0.0 < 42
         */
        final var l2TranslatedPredicate = l2Translator.translate(pPredicate);
        Assertions.assertEquals(new RelOpValue.LtFn().encapsulate(List.of(fv(p_, 0, 0), LiteralValue.ofScalar(42))), l2TranslatedPredicate);

        final var l2ExpectedMapping = Map.of(rcv(fv(p_, 1, 0)), rcv(fv(p_, 1, 0)));
        final var l2m3 = l2Translator.calculateMaxMatches(l2TranslatedQueryValue, r_v);
        Assertions.assertEquals(l2ExpectedMapping, l2m3.getMapping());
        Assertions.assertEquals(expectedL2TranslatedQueryValue, l2m3.getQueryResultValue());
        Assertions.assertEquals(r_v, l2m3.getCandidateResultValue());
    }
}
