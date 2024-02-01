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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.PullUpValueRuleSet;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
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

    // translator needs to return a m3 to upper translator.
    // translator tra


    @Nonnull
    BiMap<Value, Value> translateValue(@Nonnull final Value source,
                                       @Nonnull final CorrelationIdentifier sourceAlias,
                                       @Nonnull final Value target,
                                       @Nonnull final CorrelationIdentifier targetAlias,
                                       @Nonnull final Map<Value, Value> maxMatchMap) {
        final BiMap<Value, Value> result = HashBiMap.create();

        // translate source value using the max match map. (holy value).
        // the identities alias map is needed so we can do semantic equalities
        // when calling 'replace'
        final var boundIdentitiesMap = AliasMap.identitiesFor(source.getCorrelatedTo());
        final var translatedSource = Verify.verifyNotNull(source.replace(value ->
                maxMatchMap.entrySet().stream().filter(maxMatchMapItem -> {
                    final var aliasMap = AliasMap.identitiesFor(Sets.union(maxMatchMapItem.getKey().getCorrelatedTo(), source.getCorrelatedTo()));
                    return maxMatchMapItem.getKey().semanticEquals(value, aliasMap);
                }).map(Map.Entry::getValue).findAny().orElse(value)));
        // Now that the source is translated, we can look up the max match between the translated source
        // and the value.
        System.out.println("replaced value : " + translatedSource);
        translatedSource.pruningIterator(needle -> {

            var pulledUpSourceMap = translatedSource.pullUp(List.of(needle), boundIdentitiesMap, Set.of(), sourceAlias);


            final var toBeMappedSource = pulledUpSourceMap.get(needle);
            //System.out.println("the map is " + pulledUpSourceMap);
            if (toBeMappedSource == null) { // degenerate case, the iterator starts with the entire value which can not be pulled through itself.
                System.out.println("degenerate case");

                // System.out.println("needle " + needle);
                //System.out.println("needle" + needle);
                //System.out.println(pulledUpSourceMap);
                return true;  // return true, descend in children.
            }
            {
                // cache lookup and skip if found ...
                System.out.println("------------------------------");

                final var toBeMappedSourceAliasMap = AliasMap.identitiesFor(toBeMappedSource.getCorrelatedTo());
                //System.out.println("HERE IS THE MAPPED SOURCE" + toBeMappedSource);
                if (result.keySet().stream().anyMatch(key -> key.semanticEquals(toBeMappedSource, toBeMappedSourceAliasMap))) {
                    // A match has been already established for a semantically-identical source Value
                    // encountered previously in pre-order traversal.
                    System.out.println("<-- retracting because I already found " + needle + " before");
                    return false;
                }
            }

            {
                // find max match
                System.out.println("--> find max match for " + needle);

                // look up the target value in pre-order traversal mode (so we can find the maximum match available).
                final var foundMaybe = target.stream().filter(targetItem -> needle.semanticEquals(targetItem, AliasMap.identitiesFor(needle.getCorrelatedTo()))).findAny();
                if (foundMaybe.isEmpty()) {
                    System.out.println("could not find matches for " + needle + " => descend into the children");

                    // if the needle is not descendible or has no children, throw an exception because it is impossible to match it.
                    if (Iterables.isEmpty(needle.getChildren())) {
                        throw new RecordCoreException(String.format("Could not find a match for value %s", needle));
                    } else {
                        // Could not find a match for the current node, break it down and search for matches to its constituents.
                        return true;
                    }
                } else {
                    final var found = foundMaybe.get();
                    final var pulledUpTargetMap = target.pullUp(List.of(found), boundIdentitiesMap, Set.of(), targetAlias);
                    final var toBeMappedTarget = pulledUpTargetMap.get(found);
                    if (toBeMappedTarget == null) {
                        throw new RecordCoreException(String.format("could not pull up %s", found));
                    }
                    result.put(toBeMappedSource, toBeMappedTarget);
                    System.out.println("found match for " + needle + " that is " + found);
                    return false; // prune children of needle, because a match is already found!
                }
            }
        }).forEachRemaining(v -> {
        });
        return result;
    }


    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier P = CorrelationIdentifier.of("P");
    @SuppressWarnings("checkstyle:MemberName")
    private final CorrelationIdentifier Q = CorrelationIdentifier.of("Q");

    // private static final Value someCurrentValue = ObjectValue.of(P, getRecordType());

    @Test
    public void testValueTranslation1() throws Exception {
        final var t = qov("T", getTType());
        final var t_ = qov("T'", getTType());

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

        final var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_));

        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 0, 0),
                fv(p, 1), /* -> */ fv(p_, 0, 1),
                fv(p, 2, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 3), /* -> */ fv(p_, 2));
        Assertions.assertEquals(expectedMapping, result);
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

        final var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_));

        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 0, 0),
                fv(p, 3), /* -> */ fv(p_, 0, 1),
                fv(p, 4, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 5), /* -> */ fv(p_, 2));

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

        final var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_));
        // let's verify
        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        // this should be the holy value that replaces P and sits on top of P'.
        final var expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 3, 0),
                fv(p, 1, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 2), /* -> */ fv(p_, 2));

        Assertions.assertEquals(expectedMapping, result);
    }

    @Test
    public void testValueTranslation4() throws Exception {
        final var t = qov("T", getTType());
        final var s = qov("S", getSType());
        final var t_ = qov("T'", getTType());
        final var s_ = qov("S'", getSType());

        final var pv = rcv(
                add(fv(t, "a", "q"),
                        fv(s, "x2", "z2")),
                rcv(fv(t, "b", "t")),
                fv(s, "x1", "z1")
        );

        final var p_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                rcv(add(fv(t_, "a", "q"),
                                fv(s_, "x2", "z2")),
                        fv(t_, "b", "m")),
                fv(s_, "x1", "z1"),
                fv(t_, "b", "t"),
                fv(t_, "b", "m")
        );

        final var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_, s, s_));

        // let's verify
        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 3, 0),
                fv(p, 1, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 2), /* -> */ fv(p_, 4));

        Assertions.assertEquals(expectedMapping, result);
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

        var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_));

        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        Map<Value, Value> expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 0, 0),
                fv(p, 1), /* -> */ fv(p_, 0, 1),
                fv(p, 2, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 3), /* -> */ fv(p_, 2));
        Assertions.assertEquals(expectedMapping, result);

        System.out.println("========================= Checking another value ");
        // Now, we want to do another mapping for a new pair of Values.
        final var rv = rcv(
                fv(p, 2, 0)
        );

        final var r_v = rcv(
                fv(p_, 1, 0)
        );



        result = translateValue(rv, CorrelationIdentifier.of("r"), r_v, CorrelationIdentifier.of("r_"), expectedMapping);

        final var r = qov("r", pv.getResultType());
        final var r_ = qov("r_", p_v.getResultType());

        expectedMapping = Map.of(
                r, /* -> */ r_
        );
        Assertions.assertEquals(expectedMapping, result);
    }


    void test(@Nonnull final Value source,
              @Nonnull final CorrelationIdentifier sourceAlias,
              @Nonnull final Map<Value, Value> maxMatchMap) {
        // translate source value using the max match map. (holy value).
        // the identities alias map is needed so we can do semantic equalities
        // when calling 'replace'
        final var boundIdentitiesMap = AliasMap.identitiesFor(source.getCorrelatedTo());
        final var translatedSource = Verify.verifyNotNull(source.replace(value ->
                maxMatchMap.entrySet().stream().filter(maxMatchMapItem -> {
                    final var aliasMap = AliasMap.identitiesFor(Sets.union(maxMatchMapItem.getKey().getCorrelatedTo(), source.getCorrelatedTo()));
                    return maxMatchMapItem.getKey().semanticEquals(value, aliasMap);
                }).map(Map.Entry::getValue).findAny().orElse(value)));
        // Now that the source is translated, we can look up the max match between the translated source
        // and the value.
        System.out.println("replaced value : " + translatedSource);
        var pulledUpSourceMap = translatedSource.pullUp(List.of(translatedSource), boundIdentitiesMap, Set.of(), sourceAlias);
        Assertions.assertNotNull(pulledUpSourceMap.get(translatedSource));
    }

    void test2(@Nonnull final Value source,
               @Nonnull final CorrelationIdentifier sourceAlias) {
        // translate source value using the max match map. (holy value).
        // the identities alias map is needed so we can do semantic equalities
        // when calling 'replace'
        final var boundIdentitiesMap = AliasMap.identitiesFor(source.getCorrelatedTo());
        var pulledUpSourceMap = source.pullUp(List.of(source), boundIdentitiesMap, Set.of(), sourceAlias);
        Assertions.assertNotNull(pulledUpSourceMap.get(source));
    }

    @Test
    public void testBug2() throws Exception {
        final var t = qov("T", getTType());
        final var pv = rcv(
                fv(t, "a", "q"),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );
        final var p = qov("p", pv.getResultType());
        // Now, we want to do another mapping for a new pair of Values.
        final var rv = rcv(
                fv(p, 2, 0)
        );
        for (int i = 0; i < 100; i++) {
            System.out.println(" i = " + i);
            test2(rv, CorrelationIdentifier.of("r"));
        }
    }

    @Test
    public void testBug3() throws Exception {
        final var t = qov("T", getTType());
        final var pv = rcv(
                fv(t, "a", "q"),
                fv(t, "a", "r"),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s")
        );
        final var p = qov("p", pv.getResultType());
        // Now, we want to do another mapping for a new pair of Values.
        final var rv = rcv(
                fv(p, 2, 0)
        );
        for (int i = 0; i < 100; i++) {
            System.out.println("i = " + i);
            test2(rv, CorrelationIdentifier.of("r"));
        }
    }

    @Disabled
    @Test
    public void testBug() throws Exception {

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

        var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_));

        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        Map<Value, Value> expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 0, 0),
                fv(p, 1), /* -> */ fv(p_, 0, 1),
                fv(p, 2, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 3), /* -> */ fv(p_, 2));
        Assertions.assertEquals(expectedMapping, result);

        System.out.println("========================= Checking another value ");
        // Now, we want to do another mapping for a new pair of Values.
        final var rv = rcv(
                fv(p, 2, 0)
        );

        final var r_v = rcv(
                fv(p_, 1, 0)
        );

        //result = translateValue(rv, CorrelationIdentifier.of("r"), r_v, CorrelationIdentifier.of("r_"), expectedMapping);

        for (int i = 0; i < 100; i++) {
            test2(rv, CorrelationIdentifier.of("r"));
        }
    }

    @Test
    public void testValueTranslation6() throws Exception {
        final var t = qov("T", getTType());
        final var t_ = qov("T'", getTType());

        final var pv = rcv(
                add(fv(t, "a", "q"),
                        fv(t, "a", "r")),
                rcv(fv(t, "b", "t")),
                fv(t, "j", "s"),
                add(fv(t_, "a", "q"),
                        fv(t_, "a", "r"))
        );

        final var p_v = rcv(
                rcv(fv(t_, "a", "q"),
                        fv(t_, "a", "r")),
                rcv(fv(t_, "b", "t"),
                        fv(t_, "b", "m")),
                fv(t_, "j", "s"),
                add(add(fv(t_, "a", "q"),
                        fv(t_, "a", "r")), // matching this is wrong.
                    fv(t_, "b", "m")),
                add(fv(t_, "a", "q"),
                        fv(t_, "a", "r")), // however, matching this is correct
                fv(t_, "b", "t"),
                fv(t_, "b", "m"),
                add(fv(t_, "a", "q"),
                        fv(t_, "a", "r"))
        );

        final var result = translateValue(pv, CorrelationIdentifier.of("p"), p_v, CorrelationIdentifier.of("p_"), Map.of(t, t_));
        // let's verify
        final var p = qov("p", pv.getResultType());
        final var p_ = qov("p_", p_v.getResultType());

        final var expectedMapping = Map.of(
                fv(p, 0), /* -> */ fv(p_, 4),
                fv(p, 1, 0), /* -> */ fv(p_, 1, 0),
                fv(p, 2), /* -> */ fv(p_, 2));

        Assertions.assertEquals(expectedMapping, result);
    }

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
}
