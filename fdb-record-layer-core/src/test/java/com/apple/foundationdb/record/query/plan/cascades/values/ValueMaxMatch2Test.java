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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
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
 * Tests for value mapping.
 */
public class ValueMaxMatch2Test {

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
        return QuantifiedObjectValue.of(CorrelationIdentifier.of(name), type);
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

        @Nonnull
        public static MaxMatchMap trivialMapping(@Nonnull final QuantifiedObjectValue queryResult, @Nonnull final QuantifiedObjectValue candidateResult) {
            return new MaxMatchMap(ImmutableMap.of(queryResult, candidateResult), queryResult, candidateResult);
        }
    }

    class Translator2 {

        @Nonnull
        private final MaxMatchMap maxMatchMapBelow;

        @Nonnull
        private final CorrelationIdentifier queryCorrelation;

        @Nonnull
        private final CorrelationIdentifier candidateCorrelation;

        // todo: constant aliases.
        Translator2(@Nonnull final MaxMatchMap maxMatchMapBelow,
                    @Nonnull final CorrelationIdentifier queryCorrelation,
                    @Nonnull final CorrelationIdentifier candidateCorrelation) {
            this.maxMatchMapBelow = maxMatchMapBelow;
            this.queryCorrelation = queryCorrelation;
            this.candidateCorrelation = candidateCorrelation;
        }

        @Nonnull
        Value translate(@Nonnull final Value value) {
            final var belowMapping = maxMatchMapBelow.getMapping();
            final var belowCandidateResultValue = maxMatchMapBelow.getCandidateResult();
            // Map each value of the given maximum match map to the pulled up representation
            // of it in the candidate side.
            // for example:
            //   if the max match map contains (u -> v, w -> x)
            //    and the candidate value (p') is: (x, v)
            // the pulled up max match map will be
            //    (u -> p'.1, v -> p'.0)
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

            // Create a translated value that replaces the query value constituents with the correponding domain
            // values from the pulled up max match map from above.
            // for example, if the query value (p) is (u+w, u)
            //   the translated value will be (p'.1 + p'.0, p'.1)
            // i.e. the value p is now described solely with p'.
            final var result = Verify.verifyNotNull(value.replace(valuePart ->
                    pulledUpMaxMatchMap.entrySet().stream().filter(maxMatchMapItem -> {
                        final var aliasMap = AliasMap.identitiesFor(Sets.union(maxMatchMapItem.getKey().getCorrelatedTo(), valuePart.getCorrelatedTo()));
                        return maxMatchMapItem.getKey().semanticEquals(valuePart, aliasMap);
                    }).map(Map.Entry::getValue).findAny().orElse(valuePart)));
            return result;
        }

        @Nonnull
        MaxMatchMap constructMaxMatchMap(@Nonnull final Value queryValue, @Nonnull final Value candidateValue) {
            final var belowMapping = maxMatchMapBelow.getMapping();
            final BiMap<Value, Value> newMapping = HashBiMap.create();
            queryValue.preOrderPruningIterator(queryValuePart -> {
                final var needle = queryValuePart.replace(queryValueInternalPart -> {
                    final var candidateValueMatch = belowMapping.get(queryValueInternalPart);
                    return candidateValueMatch == null ? queryValueInternalPart : candidateValueMatch;
                });

                // now that we have rewritten this query value part using candidate value(s) we proceed to look it up in the candidate value.
                final var match = candidateValue
                        .preOrderStream()
                        .filter(candidateValuePart -> candidateValuePart.semanticEquals(needle, AliasMap.identitiesFor(candidateValuePart.getCorrelatedTo())))
                        .findAny();

                match.ifPresent(value -> newMapping.put(queryValuePart, value));
                return match.isPresent();
            });

            return new MaxMatchMap(newMapping, queryValue, candidateValue);
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

        final var t = qov("T", getTType());
        final var t_ = qov("T'", getTType());

        final var M3_1 = MaxMatchMap.trivialMapping(t, t_);

        /**
         *   p                p'
         * (pv)             (pv')
         *   t               t'
         */

        final var tAlias = CorrelationIdentifier.of("t");
        final var t_Alias = CorrelationIdentifier.of("t_");

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

        final var translator = new Translator2(M3_1, tAlias, t_Alias);

        // let's see if we can translate some predicates correctly.
        final var queryPredicate = (Value)new RelOpValue.EqualsFn().encapsulate(List.of(fv(t, "a", "q"), LiteralValue.ofScalar("hello")));
        final var translatedQueryPredicate = translator.translate(queryPredicate);

        // todo: better assertion.
        Assertions.assertNotNull(translatedQueryPredicate);

        final var M3_2 = translator.constructMaxMatchMap(pv, p_v);

        // todo: better assertion.
        Assertions.assertNotNull(M3_2);

//        final var p = qov("p", pv.getResultType());
//        final var p_ = qov("p_", p_v.getResultType());
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
