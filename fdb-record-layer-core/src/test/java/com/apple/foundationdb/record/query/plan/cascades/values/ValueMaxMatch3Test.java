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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests for value mapping.
 */
public class ValueMaxMatch3Test {

    // ------------- Utility functions.

    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier P = CorrelationIdentifier.of("P");
    @SuppressWarnings("checkstyle:MemberName")
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
    private Type.Record getSType() {
        return r(
                f("x1", r("y1", "z1")),
                f("x2", r("y2", "z2"))
        );
    }

    @SuppressWarnings("checkstyle:MethodName")
    private QuantifiedObjectValue qov(@Nonnull final String name) {
        return qov(name, getRecordType());
    }

    @SuppressWarnings("checkstyle:MethodName")
    private QuantifiedObjectValue qov(@Nonnull final String name, @Nonnull final Type type) {
        return qov(CorrelationIdentifier.of(name), type);
    }

    @SuppressWarnings("checkstyle:MethodName")
    private QuantifiedObjectValue qov(@Nonnull final CorrelationIdentifier name, @Nonnull final Type type) {
        return QuantifiedObjectValue.of(name, type);
    }

    @SuppressWarnings("checkstyle:MethodName")
    private Type.Record r(String... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .map(this::f)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private Type.Record r(Type.Record.Field... fields) {
        return Type.Record
                .fromFields(Arrays.stream(fields)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private Type.Record.Field f(String name) {
        return Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of(name));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private Type.Record.Field f(String name, @Nonnull final Type type) {
        return Type.Record.Field.of(type, Optional.of(name));
    }


    private Value rcv(Value... values) {
        return RecordConstructorValue.ofUnnamed(Arrays.stream(values).collect(Collectors.toList()));
    }

    @SuppressWarnings("checkstyle:MethodName")
    private FieldValue fv(@Nonnull final Value base, String... name) {
        return fvInternal(base, name.length - 1, name);
    }

    private FieldValue fv(String... name) {
        return fvInternal(ObjectValue.of(P, getRecordType()), name.length - 1, name);
    }

    private FieldValue fv(@Nonnull final Value base, Integer... indexes) {
        return fvInternal(base, indexes.length - 1, indexes);
    }

    private FieldValue fvInternal(Value value, int index, String... name) {
        if (index == 0) {
            return FieldValue.ofFieldNameAndFuseIfPossible(value, name[0]);
        }
        return FieldValue.ofFieldNameAndFuseIfPossible(fvInternal(value, index - 1, name), name[index]);
    }

    private FieldValue fvInternal(Value value, int index, Integer... indexes) {
        if (index == 0) {
            return FieldValue.ofOrdinalNumber(value, indexes[0]);
        }
        return FieldValue.ofOrdinalNumberAndFuseIfPossible(fvInternal(value, index - 1, indexes), indexes[index]);
    }

    private Value add(Value... values) {
        Verify.verify(values.length == 2);
        return new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_SS,
                values[0], values[1]);
    }


    // ------------- Tests got here.

    static class MaxMatchMap {
        @Nonnull
        private final Map<Value, Value> mapping;
        @Nonnull
        private final Value queryResult; // in terms of the candidate quantifiers.
        @Nonnull
        private final Value candidateResult;

        MaxMatchMap(@Nonnull final Map<Value, Value> mapping,
                    @Nonnull final Value queryResult,
                    @Nonnull final Value candidateResult) {
            this.mapping = mapping;
            this.queryResult = queryResult;
            this.candidateResult = candidateResult;
        }

        @Nonnull
        public Value getCandidateResult() {
            return candidateResult;
        }

        @Nonnull
        public Value getQueryResult() {
            return queryResult;
        }

        @Nonnull
        public Map<Value, Value> getMapping() {
            return mapping;
        }
    }

    static class Translator3 {

        @Nullable
        private final MaxMatchMap maxMatchMapBelow;

        @Nonnull
        private final CorrelationIdentifier queryCorrelation;

        @Nonnull
        private final CorrelationIdentifier candidateCorrelation;

        // todo: constant aliases.
        Translator3(@Nullable final MaxMatchMap maxMatchMapBelow,
                    @Nonnull final CorrelationIdentifier queryCorrelation,
                    @Nonnull final CorrelationIdentifier candidateCorrelation) {
            this.maxMatchMapBelow = maxMatchMapBelow;
            this.queryCorrelation = queryCorrelation;
            this.candidateCorrelation = candidateCorrelation;
        }

        @Nonnull
        Value translate(@Nonnull final Value value) {
            if (maxMatchMapBelow == null) {
                return value.translateCorrelations(TranslationMap.rebaseWithAliasMap(AliasMap.of(queryCorrelation, candidateCorrelation)));
            }
            final var holyValue = createHolyValue();
            final var result = value.translateCorrelations(TranslationMap.builder().when(queryCorrelation).then(candidateCorrelation, (src, tgt, quantifiedValue) -> holyValue).build());
            final var simplifiedResult = result.simplify(AliasMap.emptyMap(), result.getCorrelatedTo());
            return simplifiedResult;
        }

        // todo memoize.
        private Value createHolyValue() {
            final var belowMapping = Verify.verifyNotNull(maxMatchMapBelow).getMapping();
            final var belowCandidateResultValue = maxMatchMapBelow.getCandidateResult();
            final Map<Value, Value> pulledUpMaxMatchMap = belowMapping.entrySet().stream().map(entry -> {
                final var queryPart = entry.getKey();
                final var candidatePart = entry.getValue();
                final var boundIdentitiesMap = AliasMap.identitiesFor(candidatePart.getCorrelatedTo());
                final var pulledUpCandidatesMap = belowCandidateResultValue.pullUp(List.of(candidatePart), boundIdentitiesMap, Set.of(), candidateCorrelation);
                final var pulledUpdateCandidatePart = pulledUpCandidatesMap.get(candidatePart);
                if (pulledUpdateCandidatePart == null) {
                    throw new RecordCoreException(String.format("could not pull up %s", candidatePart));
                }
                return Map.entry(queryPart, pulledUpdateCandidatePart);
            }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            final var translatedQueryValueFromBelow = Verify.verifyNotNull(maxMatchMapBelow).getQueryResult();
            final var result = Verify.verifyNotNull(translatedQueryValueFromBelow.replace(valuePart ->
                    pulledUpMaxMatchMap.entrySet().stream().filter(maxMatchMapItem -> {
                        final var aliasMap = AliasMap.identitiesFor(Sets.union(maxMatchMapItem.getKey().getCorrelatedTo(), valuePart.getCorrelatedTo()));
                        return maxMatchMapItem.getKey().semanticEquals(valuePart, aliasMap);
                    }).map(Map.Entry::getValue).findAny().orElse(valuePart)));
            return result;
        }

        @Nonnull
        MaxMatchMap constructMaxMatchMap(@Nonnull final Value rewrittenQueryValue, @Nonnull final Value candidateValue) {
            final BiMap<Value, Value> newMapping = HashBiMap.create();
            rewrittenQueryValue.preOrderPruningIterator(queryValuePart -> {
                // now that we have rewritten this query value part using candidate value(s) we proceed to look it up in the candidate value.
                final var match = candidateValue
                        .preOrderStream()
                        .filter(candidateValuePart -> candidateValuePart.semanticEquals(queryValuePart, AliasMap.identitiesFor(candidateValuePart.getCorrelatedTo())))
                        .findAny();

                match.ifPresent(value -> newMapping.put(queryValuePart, value));
                return match.isEmpty();
            }).forEachRemaining(ignored -> {});

            return new MaxMatchMap(newMapping, rewrittenQueryValue, candidateValue);
        }
    }

    @Test
    public void testValueTranslation5() throws Exception {

        /**
         *     @Nonnull
         *     private static Type.Record getTType() {
         *         return r(
         *                 f("a", r("q", "r")),
         *                 f("b", r("t", "m")),
         *                 f("j", r("s", "q"))
         *         );
         *     }
         */

        final var tAlias = CorrelationIdentifier.of("T");
        final var t_Alias = CorrelationIdentifier.of("T'");

        final var t = qov(tAlias, getTType());
        final var t_ = qov(t_Alias, getTType());

        /**
         *   p                p'
         * (pv)             (pv')
         *   t               t'
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

        final var translator = new Translator3(null, tAlias, t_Alias);

        final var translatedValue = translator.translate(pv);

        // let's see if we can translate some predicates correctly.
//        final var queryPredicate = (Value)new RelOpValue.EqualsFn().encapsulate(List.of(fv(t, "a", "q"), LiteralValue.ofScalar("hello")));
//        final var translatedQueryPredicate = translator.translate(queryPredicate);
//
        // todo: better assertion.
//        Assertions.assertNotNull(translatedQueryPredicate);

        final var M3_2 = translator.constructMaxMatchMap(translatedValue, p_v);

        // todo: better assertion.
        Assertions.assertNotNull(M3_2);

        final var pAlias = CorrelationIdentifier.of("p");
        final var p_Alias = CorrelationIdentifier.of("p_");

        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var T3 = new Translator3(M3_2, pAlias, p_Alias);


        // let's see if we can translate some predicates correctly.
        final var queryPredicate = (Value)new RelOpValue.EqualsFn().encapsulate(List.of(fv(p, 0), LiteralValue.ofScalar("hello")));
        final var translatedQueryPredicate = T3.translate(queryPredicate);

        final var rv = rcv(
                fv(p, 2, 0)
        );

        final var r_v = rcv(
                fv(p_, 1, 0)
        );


        final var translatedRv = T3.translate(rv);
        final var M3_3 = T3.constructMaxMatchMap(translatedRv, r_v);

        Assertions.assertNotNull(M3_3);

//
//        Map<Value, Value> expectedMapping = Map.of(
//                fv(p, 0), /* -> */ fv(p_, 0, 0),
//                fv(p, 1), /* -> */ fv(p_, 0, 1),
//                fv(p, 2, 0), /* -> */ fv(p_, 1, 0),
//                fv(p, 3), /* -> */ fv(p_, 2));
//        Assertions.assertEquals(expectedMapping, result);
//
//        System.out.println("========================= Checking another value ");
//        // Now, we want to do another mapping for a new pair of Values.
//        final var rv = rcv(
//                fv(p, 2, 0)
//        );
//
//        final var r_v = rcv(
//                fv(p_, 1, 0)
//        );
//
//        final var translator2 = new Translator(expectedMapping, p, rv, CorrelationIdentifier.of("r"), p_, r_v, CorrelationIdentifier.of("r_"));
//        expectedMapping = translator2.getMaxMatchMap();
//
//        final var r = qov("r", pv.getResultType());
//        final var r_ = qov("r_", p_v.getResultType());
//
//        expectedMapping = Map.of(
//                r, /* -> */ r_
//        );
//        Assertions.assertEquals(expectedMapping, result);
    }
}
