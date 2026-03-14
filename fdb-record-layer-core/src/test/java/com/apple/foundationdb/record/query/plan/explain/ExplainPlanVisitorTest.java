/*
 * ExplainPlanVisitorTest.java
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

package com.apple.foundationdb.record.query.plan.explain;

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
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.TextScan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.rules.QueryPredicateSimplificationRuleTest.RandomPredicateGenerator;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
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
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of the {@link ExplainPlanVisitor} visitor. This test fixture has the ability to generate random plan
 * trees, and then it attempts to compare the expected plan string representations built by those random plans with
 * the plan strings that the visitor produces. The visitor also has logic to terminate early if it has reached
 * a maximum size, which this also tests out on random plans.
 */
public class ExplainPlanVisitorTest {
    private static final Logger logger = LoggerFactory.getLogger(ExplainPlanVisitorTest.class);

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

    static Stream<Long> manyRandomSeeds(long seedSeed, int count) {
        Random r = new Random(seedSeed);
        return LongStream.generate(r::nextLong)
                .limit(count)
                .boxed();
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
        return NonnullPair.of(new RecordQueryScanPlan(comparisons.getLeft(), reverse), "SCAN(" + comparisons.getRight() + ")");
    }

    private static NonnullPair<RecordQueryPlan, String> randomIndexPlan(Random r) {
        final NonnullPair<RecordQueryPlan, String> childPlan = randomIndexDetails(r);
        return NonnullPair.of(childPlan.getLeft(), "ISCAN(" + childPlan.getRight() + ")");
    }

    private static NonnullPair<RecordQueryPlan, String> randomIndexDetails(Random r) {
        Pair<ScanComparisons, String> comparisons = randomScanComparisons(r);
        IndexScanType scanType = randomChoice(r, List.of(IndexScanType.BY_VALUE, IndexScanType.BY_RANK, IndexScanType.BY_GROUP, IndexScanType.BY_VALUE_OVER_SCAN));
        IndexScanParameters scanParameters = IndexScanComparisons.byValue(comparisons.getLeft(), scanType);
        String indexName = randomIndexName(r);
        boolean reverse = r.nextBoolean();
        return NonnullPair.of(new RecordQueryIndexPlan(indexName, scanParameters, reverse),
                indexName + " " + comparisons.getRight() + (scanType == IndexScanType.BY_VALUE ? "" : (" " + scanType)) + (reverse ? " REVERSE" : ""));
    }

    private static NonnullPair<RecordQueryPlan, String> randomTextIndexPlan(Random r) {
        final NonnullPair<RecordQueryPlan, String> childPlan = randomTextIndexDetails(r);
        return NonnullPair.of(childPlan.getLeft(), "TISCAN(" + childPlan.getRight() + ")");
    }

    private static NonnullPair<RecordQueryPlan, String> randomTextIndexDetails(Random r) {
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
        return NonnullPair.of(new RecordQueryTextIndexPlan(indexName, textScan, r.nextBoolean()),
                indexName + ", " + groupComparisons + ", " + textComparison + ", " + (suffixComparisons == null ? "NULL" : suffixComparisons));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomCoveringIndexPlan(@Nonnull Random r) {
        NonnullPair<RecordQueryPlan, String> childPlan;
        if (r.nextDouble() < 0.8) {
            childPlan = randomIndexDetails(r);
        } else {
            childPlan = randomTextIndexDetails(r);
        }

        assertThat(childPlan.getLeft(), Matchers.instanceOf(RecordQueryPlanWithIndex.class));
        RecordQueryPlanWithIndex planWithIndex = (RecordQueryPlanWithIndex) childPlan.getLeft();
        IndexKeyValueToPartialRecord partialRecord = IndexKeyValueToPartialRecord.newBuilder(TestRecords1Proto.MySimpleRecord.getDescriptor())
                .addField("str_value_indexed", r.nextBoolean() ? IndexKeyValueToPartialRecord.TupleSource.KEY : IndexKeyValueToPartialRecord.TupleSource.VALUE, new AvailableFields.TruePredicate(), ImmutableIntArray.builder().add(r.nextInt(10)).build(), null)
                .build();
        return NonnullPair.of(new RecordQueryCoveringIndexPlan(planWithIndex, randomTypeName(r), planWithIndex.getAvailableFields(), partialRecord),
                "COVERING(" + childPlan.getRight() + " -> " + partialRecord + ")");
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomExplodePlan(@Nonnull Random r) {
        Value collectionValue;
        if (r.nextBoolean()) {
            collectionValue = LiteralValue.ofList(List.of(1, 2, 3, 4, 5));
        } else {
            collectionValue = ConstantObjectValue.of(CorrelationIdentifier.uniqueId(), "__const_" + r.nextInt(10), new Type.Array(Type.primitiveType(Type.TypeCode.LONG, false)));
        }
        return NonnullPair.of(new RecordQueryExplodePlan(collectionValue), "EXPLODE " + collectionValue);
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomLoadByKeysPlan(@Nonnull Random r) {
        if (r.nextBoolean()) {
            String parameterName = randomParameterName(r);
            return NonnullPair.of(new RecordQueryLoadByKeysPlan(parameterName), "BYKEYS $" + parameterName);
        } else {
            List<Tuple> primaryKeys = new ArrayList<>();
            int primaryKeyCount = r.nextInt(10);
            for (int i = 0; i < primaryKeyCount; i++) {
                primaryKeys.add(Tuple.from(r.nextBoolean(), r.nextLong()));
            }
            return NonnullPair.of(new RecordQueryLoadByKeysPlan(primaryKeys), "BYKEYS " + primaryKeys);
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
                childPlan.getRight() + " | QCFILTER " + (filters.size() == 1 ? Iterables.getOnlyElement(filters) : Query.and(filters)));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomFetchFromPartialRecordPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        return NonnullPair.of(new RecordQueryFetchFromPartialRecordPlan(childPlan.getLeft(), TranslateValueFunction.unableToTranslate(), Type.primitiveType(Type.TypeCode.UNKNOWN), RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY),
                childPlan.getRight() + " | FETCH");
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomMapPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        Value resultValue = LiteralValue.ofScalar("a_value");
        return NonnullPair.of(new RecordQueryMapPlan(Quantifier.physical(Reference.plannedOf(childPlan.getLeft())), resultValue),
                childPlan.getRight() + " | MAP " + resultValue);
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
            valueString = values.stream().map(Object::toString).collect(Collectors.joining(", "));
        } else {
            Comparisons.Comparison comparison;
            if (r.nextBoolean()) {
                comparison = new Comparisons.ListComparison(Comparisons.Type.IN, values);
                valueString = "IN " + values;
            } else {
                comparison = new Comparisons.ParameterComparison(Comparisons.Type.IN, outerParamName);
                valueString = "IN $" + outerParamName;
            }
            inJoinPlan = new RecordQueryInComparandJoinPlan(childPlan.getLeft(), innerParam, Bindings.Internal.IN, comparison, sortValues, sortReverse);
        }

        return NonnullPair.of(inJoinPlan,
                "[" + valueString + (sortValues ? " SORTED" : "") + (sortValues && sortReverse ? " DESC" : "") + "] | INJOIN " + innerParam + " -> { " + childPlan.getRight() + " }");
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomPrimaryKeyUnorderedDistinctPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        return NonnullPair.of(new RecordQueryUnorderedPrimaryKeyDistinctPlan(childPlan.getLeft()), childPlan.getRight() + " | DISTINCT BY PK");
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomUnorderedDistinctPlan(@Nonnull Random r, double decay) {
        Pair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        KeyExpression expression = Key.Expressions.field(randomAlphabetic(r, 5, 10));
        return NonnullPair.of(new RecordQueryUnorderedDistinctPlan(childPlan.getLeft(), expression), childPlan.getRight() + " | DISTINCT BY " + expression);
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomSortPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        KeyExpression expression = Key.Expressions.field(randomAlphabetic(r, 5, 10));
        boolean reverse = r.nextBoolean();
        return NonnullPair.of(new RecordQuerySortPlan(childPlan.getLeft(), new RecordQuerySortKey(expression, reverse)), childPlan.getRight() + " | SORT BY " + expression + (reverse ? " DESC" : ""));
    }

    @Nonnull
    private static NonnullPair<RecordQueryPlan, String> randomTypeFilterPlan(@Nonnull Random r, double decay) {
        NonnullPair<RecordQueryPlan, String> childPlan = randomPlanAndString(r, decay);
        int typeCount = r.nextInt(3) + 1;
        Set<String> types = new HashSet<>();
        for (int i = 0; i < typeCount; i++) {
            types.add(randomTypeName(r));
        }
        return NonnullPair.of(new RecordQueryTypeFilterPlan(childPlan.getLeft(), types), childPlan.getRight() + " | TFILTER " + String.join(", ", types));
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
            return NonnullPair.of(RecordQueryUnorderedUnionPlan.from(plans), String.join(" ⊎ ", strings));
        } else if (choice < 0.6) {
            KeyExpression comparisonKey = Key.Expressions.field(randomFieldName(r));
            return NonnullPair.of(RecordQueryIntersectionPlan.from(plans, comparisonKey), String.join(" ∩ ", strings) + " COMPARE BY " + comparisonKey);
        } else {
            KeyExpression comparisonKey = Key.Expressions.field(randomFieldName(r));
            return NonnullPair.of(RecordQueryUnionPlan.from(plans, comparisonKey, true),
                    String.join(" ∪ ", strings) + " COMPARE BY " + comparisonKey);
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

    @Nonnull
    static Stream<Long> randomPlanRepresentation() {
        long seedSeed = RandomizedTestUtils.includeRandomTests() ? System.nanoTime() : 0x5ca1ab1eL;
        return manyRandomSeeds(seedSeed, 500);
    }

    @ParameterizedTest(name = "randomPlanRepresentation[seed={0}]")
    @MethodSource
    void randomPlanRepresentation(long seed) {
        Random r = new Random(seed);
        logger.info("randomPlanRepresentation seed {}", seed);
        NonnullPair<RecordQueryPlan, String> planAndString = randomPlanAndString(r);
        assertEquals(planAndString.getRight(), planAndString.getLeft().toString());
        assertEquals(planAndString.getRight(), ExplainPlanVisitor.toStringForDebugging(planAndString.getLeft()));
    }

    @ParameterizedTest(name = "randomPlanDotRepresentation[seed={0}]")
    @MethodSource("randomPlanRepresentation")
    void randomPlanDotRepresentation(long seed) {
        Random r = new Random(seed);
        logger.info("randomPlanDotRepresentation seed {}", seed);
        NonnullPair<RecordQueryPlan, String> planAndString = randomPlanAndString(r);
        logger.info("dot={}", PlannerGraphVisitor.internalGraphicalExplain(planAndString.getLeft()));
    }

    @Nonnull
    static Stream<Long> shrinkPlansToFit() {
        long seedSeed = RandomizedTestUtils.includeRandomTests() ? System.nanoTime() : 0x0fdb5ca1eL;
        return manyRandomSeeds(seedSeed, 100);
    }

    @ParameterizedTest(name = "shrinkPlansToFit[seed={0}]")
    @MethodSource
    void shrinkPlansToFit(long seed) {
        Random r = new Random(seed);
        logger.info("shrinkPlansToFit seed {}", seed);
        Pair<RecordQueryPlan, String> planAndString = randomPlanAndString(r);
        RecordQueryPlan plan = planAndString.getLeft();
        String planString = planAndString.getRight();
        for (int i = 0; i < Math.min(planString.length() + 10, 1000); i++) {
            String abbreviatedString = ExplainPlanVisitor.toStringForDebugging(plan, ExplainLevel.ALL_DETAILS, i);
            assertEquals(i < planString.length() ? (planString.substring(0, i) + "...") : planString, abbreviatedString);
        }
    }

    @Nonnull
    static Stream<Long> doNotEvaluateOutsideOfLimit() {
        long seedSeed = RandomizedTestUtils.includeRandomTests() ? System.nanoTime() : 0x5ca1e0fdbL;
        return manyRandomSeeds(seedSeed, 50);
    }

    @ParameterizedTest(name = "doNotEvaluateOutsideOfLimit[seed={0}]")
    @MethodSource
    void doNotEvaluateOutsideOfLimit(long seed) {
        Random r = new Random(seed);
        logger.info("doNotEvaluateOutsideOfLimit seed={}", seed);
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

        final var unorderedUnionPlanWithoutUnstringable =
                RecordQueryUnorderedUnionPlan.from(plans);
        final var visitor = new ExplainPlanVisitor(Integer.MAX_VALUE);
        final var explainTokens = visitor.visit(unorderedUnionPlanWithoutUnstringable);
        final var minLength = explainTokens.getMinLength(ExplainLevel.STRUCTURE);
        final var withoutUnstringable =
                explainTokens.render(ExplainLevel.STRUCTURE,
                        DefaultExplainFormatter.create(DefaultExplainSymbolMap::new),
                        Integer.MAX_VALUE).toString();

        plans.add(new UnstringableQueryPlan());
        RecordQueryUnorderedUnionPlan unorderedUnionPlan = RecordQueryUnorderedUnionPlan.from(plans);

        // Constructing the full plan string should throw an error
        assertThrows(RecordCoreException.class, () -> ExplainPlanVisitor.toStringForDebugging(unorderedUnionPlan));

        // If the unstringable query isn't needed for the string representation, it shouldn't be called, so the
        // error shouldn't be thrown
        String lengthLimitedString =
                ExplainPlanVisitor.toStringForExternalExplain(unorderedUnionPlan, ExplainLevel.STRUCTURE, minLength);
        assertEquals(withoutUnstringable.substring(0, minLength) + "...", lengthLimitedString);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testDotExplainForPredicates(final long seed) {
        final WithIndentationsExplainFormatter formatter = WithIndentationsExplainFormatter.forDot(4);
        final RandomPredicateGenerator randomPredicateGenerator = new RandomPredicateGenerator(new Random(seed));
        final QueryPredicate randomPredicate = randomPredicateGenerator.generate().getPredicateUnderTest();
        final var explainTokens = randomPredicate.explain().getExplainTokens();
        final String predicateString = explainTokens.render(formatter).toString();
        final String defaultExplainPredicateString = explainTokens.render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString();
        assertEquals(defaultExplainPredicateString.replaceAll("\\s+", ""),
                predicateString.replaceAll("\\s+", ""));
    }

    @Test
    void testDotExplainForCorrelatedPredicates() {
        final Type t =
                Type.Record.fromFields(false, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("x"))));
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final QueryPredicate valuePredicate =
                new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(a, t), "x"),
                        new Comparisons.ValueComparison(Comparisons.Type.EQUALS, LiteralValue.ofScalar(0L)));
        final var explainTokens = valuePredicate.explain().getExplainTokens();
        final String defaultExplainPredicateString =
                explainTokens.render(getIndentingFormatter(DefaultExplainSymbolMap::new)).toString();
        assertEquals("a.x EQUALS 0l", defaultExplainPredicateString); // triggers an alias rendering case

        String selfContainedExplainPredicateString =
                explainTokens.render(getIndentingFormatter(ExplainSelfContainedSymbolMap::new)).toString();
        assertEquals("?a?.x EQUALS 0l", selfContainedExplainPredicateString); // triggers an error case

        final ExplainTokens explainTokensWithAliasDefinition =
                new ExplainTokens().addToString("defined:")
                        .addAliasDefinition(a).addWhitespace().addNested(explainTokens);
        selfContainedExplainPredicateString =
                explainTokensWithAliasDefinition.render(
                        getIndentingFormatter(ExplainSelfContainedSymbolMap::new)).toString();
        assertEquals("defined:q0 q0.x EQUALS 0l", selfContainedExplainPredicateString);
    }

    @Nonnull
    private static WithIndentationsExplainFormatter getIndentingFormatter(@Nonnull final Supplier<ExplainSymbolMap> symbolMapSupplier) {
        final WithIndentationsExplainFormatter formatter = new WithIndentationsExplainFormatter(symbolMapSupplier, 0, 50, 4);
        formatter.register();
        return formatter;
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

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return hashCodeWithoutChildren();
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return getCorrelatedToWithoutChildren();
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
            return ImmutableSet.of();
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
        public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                          final boolean shouldSimplifyValues,
                                                          @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
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
