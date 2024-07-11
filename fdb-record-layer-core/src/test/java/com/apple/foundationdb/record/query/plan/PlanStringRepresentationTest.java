/*
 * PlanStringRepresentationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInComparandJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortKey;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of the {@link PlanStringRepresentation} visitor. This test fixture has the ability to generate random plan
 * trees, and then it attempts to compare the expected plan string representations built by those random plans with
 * the plan strings that the visitor produces. The visitor also has logic to terminate early if it has reached
 * a maximum size, which this also tests out on random plans.
 */
public class PlanStringRepresentationTest {
    @Nonnull
    private static String randomAlphabetic(@Nonnull Random r, int minCount, int maxCount) {
        final int letterCount = minCount + r.nextInt(maxCount - minCount);
        char[] letters = new char[letterCount];
        for (int i = 0; i < letterCount; i++) {
            int letter = r.nextInt(26);
            letters[i] = (char)((r.nextBoolean() ? 'a' : 'A') + letter);
        }
        return new String(letters);
    }

    @Nonnull
    private static String randomParameterName(@Nonnull Random r) {
        return randomAlphabetic(r, 5, 8);
    }

    @Nonnull
    private static String randomTypeName(@Nonnull Random r) {
        return randomAlphabetic(r, 10, 15);
    }

    @Nonnull
    private static String randomIndexName(@Nonnull Random r) {
        return randomAlphabetic(r, 8, 10);
    }

    @Nonnull
    private static String randomFieldName(@Nonnull Random r) {
        return randomAlphabetic(r, 4, 9);
    }

    private static <T> T randomChoice(@Nonnull Random r, @Nonnull List<T> elements) {
        int choice = r.nextInt(elements.size());
        return elements.get(choice);
    }

    private static Comparisons.Comparison randomEqualityComparison(@Nonnull Random r) {
        double choice = r.nextDouble();
        if (choice < 0.25) {
            return new Comparisons.NullComparison(Comparisons.Type.IS_NULL);
        } else if (choice < 0.5) {
            return new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, r.nextLong());
        } else if (choice < 0.75) {
            return new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, randomParameterName(r));
        } else {
            return new RecordTypeKeyComparison(randomTypeName(r)).getComparison();
        }
    }

    private static Comparisons.Comparison randomInequalityComparison(@Nonnull Random r) {
        Comparisons.Type type = randomChoice(r, List.of(Comparisons.Type.LESS_THAN, Comparisons.Type.LESS_THAN_OR_EQUALS, Comparisons.Type.GREATER_THAN, Comparisons.Type.GREATER_THAN_OR_EQUALS));
        if (r.nextBoolean()) {
            return new Comparisons.SimpleComparison(type, r.nextLong());
        } else {
            return new Comparisons.ParameterComparison(type, randomParameterName(r));
        }
    }

    private static NonnullPair<ScanComparisons, String> randomScanComparisons(Random r) {
        final int equalityComparisonCount = r.nextInt(7);
        final ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (int i = 0; i < equalityComparisonCount; i++) {
            builder.addEqualityComparison(randomEqualityComparison(r));
        }
        final int inequalityComparisonCount = r.nextInt(2);
        for (int i = 0; i < inequalityComparisonCount; i++) {
            builder.addInequalityComparison(randomInequalityComparison(r));
        }
        ScanComparisons comparisons = builder.build();
        TupleRange tupleRange = comparisons.toTupleRangeWithoutContext();
        return NonnullPair.of(comparisons, tupleRange == null ? comparisons.toString() : tupleRange.toString());
    }

    private static NonnullPair<RecordQueryPlan, String> randomScanPlan(Random r) {
        Pair<ScanComparisons, String> comparisons = randomScanComparisons(r);
        boolean reverse = r.nextBoolean();
        return NonnullPair.of(new RecordQueryScanPlan(comparisons.getLeft(), reverse), String.format("Scan(%s)", comparisons.getRight()));
    }

    private static NonnullPair<RecordQueryPlan, String> randomIndexPlan(Random r) {
        Pair<ScanComparisons, String> comparisons = randomScanComparisons(r);
        IndexScanType scanType = randomChoice(r, List.of(IndexScanType.BY_VALUE, IndexScanType.BY_RANK, IndexScanType.BY_GROUP, IndexScanType.BY_VALUE_OVER_SCAN));
        IndexScanParameters scanParameters = IndexScanComparisons.byValue(comparisons.getLeft(), scanType);
        String indexName = randomIndexName(r);
        boolean reverse = r.nextBoolean();
        return NonnullPair.of(new RecordQueryIndexPlan(indexName, scanParameters, reverse),
                String.format("Index(%s %s%s%s)", indexName, comparisons.getRight(), scanType == IndexScanType.BY_VALUE ? "" : (" " + scanType), reverse ? " REVERSE" : ""));
    }

    private static NonnullPair<RecordQueryPlan, String> randomTextIndexPlan(Random r) {
        ScanComparisons.Builder groupComparisonsBuilder = new ScanComparisons.Builder();
        int groupingComparisons = r.nextInt(3);
        for (int i = 0; i < groupingComparisons; i++) {
            groupComparisonsBuilder.addEqualityComparison(randomEqualityComparison(r));
        }
        ScanComparisons groupComparisons = groupComparisonsBuilder.build();
        ScanComparisons suffixComparisons;
        if (r.nextBoolean()) {
            suffixComparisons = randomScanComparisons(r).getLeft();
        } else {
            suffixComparisons = null;
        }

        Comparisons.Type type = randomChoice(r, List.of(Comparisons.Type.TEXT_CONTAINS_ALL, Comparisons.Type.TEXT_CONTAINS_ANY, Comparisons.Type.TEXT_CONTAINS_ALL_PREFIXES, Comparisons.Type.TEXT_CONTAINS_ANY_PREFIX, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Comparisons.Type.TEXT_CONTAINS_PHRASE));
        Comparisons.TextComparison textComparison;
        List<String> tokens = Stream.generate(() -> randomAlphabetic(r, 5, 7)).limit(5).collect(Collectors.toList());
        String phrase = String.join(" ", tokens);
        boolean useTokens = r.nextBoolean();
        String tokenizerName = r.nextBoolean() ? null : randomAlphabetic(r, 5, 8);
        String defaultTokenizerName = randomAlphabetic(r, 5, 8);

        if (type == Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN) {
            int distance = r.nextInt(20);
            if (useTokens) {
                textComparison = new Comparisons.TextWithMaxDistanceComparison(tokens, distance, tokenizerName, defaultTokenizerName);
            } else {
                textComparison = new Comparisons.TextWithMaxDistanceComparison(phrase, distance, tokenizerName, defaultTokenizerName);
            }
        } else if (type == Comparisons.Type.TEXT_CONTAINS_ALL_PREFIXES) {
            boolean strict = r.nextBoolean();
            if (useTokens) {
                textComparison = new Comparisons.TextContainsAllPrefixesComparison(tokens, strict, tokenizerName, defaultTokenizerName);
            } else {
                textComparison = new Comparisons.TextContainsAllPrefixesComparison(phrase, strict, tokenizerName, defaultTokenizerName);
            }
        } else {
            if (useTokens) {
                textComparison = new Comparisons.TextComparison(type, tokens, tokenizerName, defaultTokenizerName);
            } else {
                textComparison = new Comparisons.TextComparison(type, phrase, tokenizerName, defaultTokenizerName);
            }
        }

        String indexName = randomIndexName(r);
        Index index = new Index(indexName, Key.Expressions.concatenateFields("text", "suffix").groupBy(Key.Expressions.field("group")), IndexTypes.TEXT);
        TextScan textScan = new TextScan(index, groupComparisons, textComparison, suffixComparisons);
        return NonnullPair.of(new RecordQueryTextIndexPlan(indexName, textScan, r.nextBoolean()), String.format("TextIndex(%s %s, %s, %s)", indexName, groupComparisons, textComparison, suffixComparisons));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomCoveringIndexPlan(@Nonnull Random r) {
        NonnullPair<RecordQueryPlan, String> childPlan;
        if (r.nextDouble() < 0.8) {
            childPlan = randomIndexPlan(r);
        } else {
            childPlan = randomTextIndexPlan(r);
        }

        assertThat(childPlan.getLeft(), Matchers.instanceOf(RecordQueryPlanWithIndex.class));
        RecordQueryPlanWithIndex planWithIndex = (RecordQueryPlanWithIndex) childPlan.getLeft();
        IndexKeyValueToPartialRecord partialRecord = IndexKeyValueToPartialRecord.newBuilder(TestRecords1Proto.MySimpleRecord.getDescriptor())
                .addField("str_value_indexed", r.nextBoolean() ? IndexKeyValueToPartialRecord.TupleSource.KEY : IndexKeyValueToPartialRecord.TupleSource.VALUE, new AvailableFields.TruePredicate(), ImmutableIntArray.builder().add(r.nextInt(10)).build(), null)
                .build();
        return NonnullPair.of(new RecordQueryCoveringIndexPlan(planWithIndex, randomTypeName(r), planWithIndex.getAvailableFields(), partialRecord),
                String.format("Covering(%s -> %s)", childPlan.getRight(), partialRecord));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomExplodePlan(@Nonnull Random r) {
        Value collectionValue;
        if (r.nextBoolean()) {
            collectionValue = LiteralValue.ofList(List.of(1, 2, 3, 4, 5));
        } else {
            collectionValue = ConstantObjectValue.of(CorrelationIdentifier.uniqueID(), "__const_" + r.nextInt(10), new Type.Array(Type.primitiveType(Type.TypeCode.LONG, false)));
        }
        return NonnullPair.of(new RecordQueryExplodePlan(collectionValue), String.format("explode([%s])", collectionValue));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomLoadByKeysPlan(@Nonnull Random r) {
        if (r.nextBoolean()) {
            String parameterName = randomParameterName(r);
            return NonnullPair.of(new RecordQueryLoadByKeysPlan(parameterName), String.format("ByKeys($%s)", parameterName));
        } else {
            List<Tuple> primaryKeys = new ArrayList<>();
            int primaryKeyCount = r.nextInt(10);
            for (int i = 0; i < primaryKeyCount; i++) {
                primaryKeys.add(Tuple.from(r.nextBoolean(), r.nextLong()));
            }
            return NonnullPair.of(new RecordQueryLoadByKeysPlan(primaryKeys), String.format("ByKeys(%s)", primaryKeys));
        }
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomFilterPlan(@Nonnull Random r, double decay) {
        Pair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        List<QueryComponent> filters = new ArrayList<>();
        int filterCount = 1 + r.nextInt(4);
        for (int i = 0; i < filterCount; i++) {
            filters.add(Query.field(randomFieldName(r)).equalsParameter(randomParameterName(r)));
        }
        return NonnullPair.of(new RecordQueryFilterPlan(childPlan.getLeft(), filters),
                String.format("%s | %s", childPlan.getRight(), filters.size() == 1 ? Iterables.getOnlyElement(filters) : Query.and(filters)));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomFetchFromPartialRecordPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        return NonnullPair.of(new RecordQueryFetchFromPartialRecordPlan(childPlan.getLeft(), TranslateValueFunction.unableToTranslate(), Type.primitiveType(Type.TypeCode.UNKNOWN), RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY),
                String.format("Fetch(%s)", childPlan.getRight()));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomMapPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        Value resultValue = LiteralValue.ofScalar("a_value");
        return NonnullPair.of(new RecordQueryMapPlan(Quantifier.physical(Reference.of(childPlan.getLeft())), resultValue),
                String.format("map(%s[%s])", childPlan.getRight(), resultValue));
    }

    @Nonnull
    public static NonnullPair<RecordQueryPlan, String> randomInJoinPlan(@Nonnull Random r, double decay) {
        Pair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        double choice = r.nextDouble();
        boolean sortValues = r.nextBoolean();
        boolean sortReverse = r.nextBoolean();
        String innerParam = Bindings.Internal.IN.bindingName(randomParameterName(r));

        List<Long> values = new ArrayList<>();
        int objCount = r.nextInt(7);
        for (int i = 0; i < objCount; i++) {
            values.add(r.nextLong());
        }
        if (sortValues) {
            values.sort(sortReverse ? ((a, b) -> -1 * Long.compare(a, b)) : Long::compare);
        }
        String outerParamName = randomParameterName(r);

        RecordQueryInJoinPlan inJoinPlan;
        String valueString;
        if (choice < 0.33) {
            inJoinPlan = new RecordQueryInParameterJoinPlan(childPlan.getLeft(), innerParam, Bindings.Internal.IN, outerParamName, sortValues, sortReverse);
            valueString = "$" + outerParamName;
        } else if (choice < 0.67) {
            inJoinPlan = new RecordQueryInValuesJoinPlan(childPlan.getLeft(), innerParam, Bindings.Internal.IN, ImmutableList.copyOf(values), sortValues, sortReverse);
            valueString = values.toString();
        } else {
            Comparisons.Comparison comparison;
            if (r.nextBoolean()) {
                comparison = new Comparisons.ListComparison(Comparisons.Type.IN, values);
                valueString = values.toString();
            } else {
                comparison = new Comparisons.ParameterComparison(Comparisons.Type.IN, outerParamName);
                valueString = "$" + outerParamName;
            }
            inJoinPlan = new RecordQueryInComparandJoinPlan(childPlan.getLeft(), innerParam, Bindings.Internal.IN, comparison, sortValues, sortReverse);
        }

        return NonnullPair.of(inJoinPlan, String.format("%s WHERE %s IN %s%s%s", childPlan.getRight(), innerParam, valueString, sortValues ? " SORTED" : "", sortValues && sortReverse ? " DESC" : ""));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomPrimaryKeyUnorderedDistinctPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        return NonnullPair.of(new RecordQueryUnorderedPrimaryKeyDistinctPlan(childPlan.getLeft()), String.format("%s | UnorderedPrimaryKeyDistinct()", childPlan.getRight()));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomUnorderedDistinctPlan(@Nonnull Random r, double decay) {
        Pair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        KeyExpression expression = Key.Expressions.field(randomAlphabetic(r, 5, 10));
        return NonnullPair.of(new RecordQueryUnorderedDistinctPlan(childPlan.getLeft(), expression), String.format("%s | UnorderedDistinct(%s)", childPlan.getRight(), expression));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomSortPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        KeyExpression expression = Key.Expressions.field(randomAlphabetic(r, 5, 10));
        boolean reverse = r.nextBoolean();
        return NonnullPair.of(new RecordQuerySortPlan(childPlan.getLeft(), new RecordQuerySortKey(expression, reverse)), String.format("%s ORDER BY %s%s", childPlan.getRight(), expression, reverse ? " DESC" : ""));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomTypeFilterPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        int typeCount = r.nextInt(3) + 1;
        Set<String> types = new HashSet<>();
        for (int i = 0; i < typeCount; i++) {
            types.add(randomTypeName(r));
        }
        return NonnullPair.of(new RecordQueryTypeFilterPlan(childPlan.getLeft(), types), String.format("%s | %s", childPlan.getRight(), types));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomUnionOrIntersectionPlan(@Nonnull Random r, double decay) {
        List<RecordQueryPlan> plans = new ArrayList<>();
        List<String> strings = new ArrayList<>();
        int planCount = r.nextInt(5) + 2;
        for (int i = 0; i < planCount; i++) {
            while (true) {
                NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
                try {
                    childPlan.getLeft().isReverse();
                    plans.add(childPlan.getLeft());
                    strings.add(childPlan.getRight());
                    break;
                } catch (RecordCoreException e) {
                    // Some plans do not have a defined reverseness. We don't want to include those here,
                    // so generate a different plan.
                }
            }
        }

        double choice = r.nextDouble();
        boolean allSameReverse = plans.stream().allMatch(plan -> plan.isReverse() == plans.get(0).isReverse());
        if (!allSameReverse || choice < 0.2) {
            return NonnullPair.of(RecordQueryUnorderedUnionPlan.from(plans), "Unordered(" + String.join(" ∪ ", strings) + ")");
        } else if (choice < 0.6) {
            KeyExpression comparisonKey = Key.Expressions.field(randomFieldName(r));
            return NonnullPair.of(RecordQueryIntersectionPlan.from(plans, comparisonKey), String.join(" ∩ ", strings));
        } else {
            KeyExpression comparisonKey = Key.Expressions.field(randomFieldName(r));
            boolean showComparisonKey = r.nextBoolean();
            String delimiter = String.format(" ∪%s ", showComparisonKey ? comparisonKey : "");
            return NonnullPair.of(RecordQueryUnionPlan.from(plans, comparisonKey, showComparisonKey), String.join(delimiter, strings));
        }
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomPlanAndString(@Nonnull Random r, double decay) {
        // Generate a random plan, but use the decay argument to avoid creating trees with too many
        // levels (and potentially hitting the maximum stack depth). Every time a plan with
        // child plans is chosen, the decay parameter is decreased, which eventually ensures that
        // a leaf plan is chosen.
        double leafChoice = r.nextDouble();
        if (leafChoice * decay < 0.2) {
            // Choose a leaf plan
            double choice = r.nextDouble();
            if (choice < 0.17) {
                return randomScanPlan(r);
            } else if (choice < 0.34) {
                return randomIndexPlan(r);
            } else if (choice < 0.5) {
                return randomTextIndexPlan(r);
            } else if (choice < 0.67) {
                return randomCoveringIndexPlan(r);
            } else if (choice < 0.84) {
                return randomExplodePlan(r);
            } else {
                return randomLoadByKeysPlan(r);
            }
        } else {
            // Choose a plan that has child plans
            double newDecay = decay * 0.8;
            double choice = r.nextDouble();
            if (choice < 0.10) {
                return randomIndexPlan(r);
            } else if (choice < 0.20) {
                return randomFetchFromPartialRecordPlan(r, newDecay);
            } else if (choice < 0.30) {
                return randomFilterPlan(r, newDecay);
            } else if (choice < 0.40) {
                return randomInJoinPlan(r, newDecay);
            } else if (choice < 0.50) {
                return randomPrimaryKeyUnorderedDistinctPlan(r, newDecay);
            } else if (choice < 0.60) {
                return randomSortPlan(r, newDecay);
            } else if (choice < 0.70) {
                return randomTypeFilterPlan(r, newDecay);
            } else if (choice < 0.80) {
                return randomMapPlan(r, newDecay);
            } else if (choice < 0.90) {
                return randomUnorderedDistinctPlan(r, newDecay);
            } else {
                return randomUnionOrIntersectionPlan(r, newDecay);
            }
        }
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomPlanAndString(@Nonnull Random r) {
        return randomPlanAndString(r, 1.0);
    }

    @RepeatedTest(500)
    void randomPlanRepresentation() {
        Random r = ThreadLocalRandom.current();
        NonnullPair<RecordQueryPlan, String> planAndString = randomPlanAndString(r);
        assertEquals(planAndString.getRight(), planAndString.getLeft().toString());
        assertEquals(planAndString.getRight(), PlanStringRepresentation.toString(planAndString.getLeft()));
    }

    @RepeatedTest(100)
    void shrinkPlansToFit() {
        Random r = ThreadLocalRandom.current();
        Pair<RecordQueryPlan, String> planAndString = randomPlanAndString(r);
        RecordQueryPlan plan = planAndString.getLeft();
        String planString = planAndString.getRight();
        for (int i = 0; i < Math.min(planString.length() + 10, 1000); i++) {
            String abbreviatedString = PlanStringRepresentation.toString(plan, i);
            assertEquals(i < planString.length() ? (planString.substring(0, i) + "...") : planString, abbreviatedString);
        }
    }

    @Test
    void doNotEvaluateOutsideOfLimit() {
        Random r = ThreadLocalRandom.current();
        List<RecordQueryPlan> plans = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            RecordQueryPlan plan;
            while (true) {
                plan = randomPlanAndString(r).getLeft();
                try {
                    plan.isReverse();
                    break;
                } catch (RecordCoreException e) {
                    // Not all plans have a defined reverse-ness. If this happens, generate a new plan
                }
            }
            plans.add(plan);
        }
        final String withoutUnstringable = RecordQueryUnorderedUnionPlan.from(plans).toString();

        plans.add(new UnstringableQueryPlan());
        RecordQueryUnorderedUnionPlan unorderedUnionPlan = RecordQueryUnorderedUnionPlan.from(plans);

        // Constructing the full plan string should throw an error
        assertThrows(UnsupportedOperationException.class, unorderedUnionPlan::toString);
        assertThrows(UnsupportedOperationException.class, () -> PlanStringRepresentation.toString(unorderedUnionPlan));

        // If the unstringable query isn't needed for the string representation, it shouldn't be called, so the
        // error shouldn't be thrown
        String lengthLimitedString = PlanStringRepresentation.toString(unorderedUnionPlan, withoutUnstringable.length() - 2);
        assertEquals(withoutUnstringable.substring(0, withoutUnstringable.length() - 2) + "...", lengthLimitedString);
    }

    /**
     * Dummy plan that has a broken {@link #toString()} implementation. This class cannot be executed, but it
     * can be used during testing to validate that the visitor does not attempt to create a string representation
     * of a plan if the size limit has already been hit.
     */
    private static class UnstringableQueryPlan implements RecordQueryPlan {
        private final boolean reverse;

        public UnstringableQueryPlan() {
            this(false);
        }

        public UnstringableQueryPlan(boolean reverse) {
            this.reverse = reverse;
        }

        @Nonnull
        @Override
        public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final byte[] continuation, @Nonnull final ExecuteProperties executeProperties) {
            throw new UnsupportedOperationException("cannot execute unstringable plan");
        }

        @Nonnull
        public String toString() {
            throw new UnsupportedOperationException("cannot call toString on unstringable plan");
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return hashCodeWithoutChildren();
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return Collections.emptySet();
        }

        @Nonnull
        @Override
        public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
            throw new UnsupportedOperationException("cannot run operation on unstringable plan");
        }

        @Nonnull
        @Override
        public Value getResultValue() {
            return new NullValue(Type.primitiveType(Type.TypeCode.UNKNOWN, true));
        }

        @Nonnull
        @Override
        public List<? extends Quantifier> getQuantifiers() {
            return Collections.emptyList();
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
            return other instanceof UnstringableQueryPlan && ((UnstringableQueryPlan)other).isReverse() == reverse;
        }

        @Override
        public int hashCodeWithoutChildren() {
            return reverse ? 1 : -1;
        }

        @Nonnull
        @Override
        public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
            return this;
        }

        @Override
        public boolean isReverse() {
            return reverse;
        }

        @Override
        public boolean hasRecordScan() {
            return false;
        }

        @Override
        public boolean hasFullRecordScan() {
            return false;
        }

        @Override
        public boolean hasIndexScan(@Nonnull final String indexName) {
            return false;
        }

        @Nonnull
        @Override
        public Set<String> getUsedIndexes() {
            return Collections.emptySet();
        }

        @Override
        public boolean hasLoadBykeys() {
            return false;
        }

        @Override
        public void logPlanStructure(final StoreTimer timer) {

        }

        @Override
        public int getComplexity() {
            return 1;
        }

        @Nonnull
        @Override
        public List<RecordQueryPlan> getChildren() {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public AvailableFields getAvailableFields() {
            return AvailableFields.NO_FIELDS;
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("serialization of this plan is not supported");
        }

        @Nonnull
        @Override
        public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("serialization of this plan is not supported");
        }
    }
}
