/*
 * ConcurrentCacheTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.util.pair.NonnullPair;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.apple.foundationdb.relational.recordlayer.query.QueryExecutionContext.OrderedLiteral.constantId;

/**
 * This tests concurrent behavior of the tertiary cache.
 */
public class ConcurrentCacheTests {

    @Nonnull
    private static final TypeRepository EMPTY_TYPE_REPO = TypeRepository.empty();

    @Nullable
    private static <V> V pickFirst(@Nonnull final Stream<V> stream) {
        return stream.findFirst().orElse(null);
    }

    @Nonnull
    private static PhysicalPlanEquivalence ppeFor(@Nonnull final QueryPlanConstraint constraint) {
        return new PhysicalPlanEquivalence(Optional.of(constraint), Optional.empty());
    }

    @Nonnull
    private static PhysicalPlanEquivalence ppeFor(@Nonnull final EvaluationContext evaluationContext) {
        return new PhysicalPlanEquivalence(Optional.empty(), Optional.of(evaluationContext));
    }

    @Nonnull
    private static EvaluationContext ecFor(int value) {
        return EvaluationContext.newBuilder().setConstant(Quantifier.constant(), Map.of(constantId(0), value)).build(EMPTY_TYPE_REPO);
    }

    @Nonnull
    private static final QueryPlanConstraint lt150Constraint = QueryPlanConstraint.ofPredicate(
            new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), constantId(0),
                    Type.primitiveType(Type.TypeCode.INT)), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 150)));

    @Nonnull
    private static final QueryPlanConstraint lt500Constraint = QueryPlanConstraint.ofPredicate(
            new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), constantId(0), Type.primitiveType(Type.TypeCode.INT)),
                    new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 500)));

    @Nonnull
    private static final QueryPlanConstraint lt1000Constraint = QueryPlanConstraint.ofPredicate(
            new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), constantId(0), Type.primitiveType(Type.TypeCode.INT)),
                    new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 1000)));

    @Nonnull
    private static String generateFullScan() {
        return "full scan";
    }

    @Nonnull
    private static String generateIScan(int boundary) {
        return "full scan with " + boundary;
    }

    private static void getOrLoadT1lt300(@Nonnull final MultiStageCache<String, String, PhysicalPlanEquivalence, String> cache) {
        final var result = cache.reduce("T1", "1", ppeFor(ecFor(300)), () -> NonnullPair.of(ppeFor(lt500Constraint),
                generateIScan(500)), s -> s + " overriden with 300", ConcurrentCacheTests::pickFirst, e -> { });
        Assertions.assertThat(result).doesNotContain("150"); // we must not scan index <150 as the returned results would be incorrect
    }

    private static void getOrLoadT1lt90(@Nonnull final MultiStageCache<String, String, PhysicalPlanEquivalence, String> cache) {
        cache.reduce("T1", "1", ppeFor(ecFor(90)), () -> NonnullPair.of(ppeFor(lt150Constraint),
                generateIScan(150)), s -> s + " overriden with 90", ConcurrentCacheTests::pickFirst, e -> { });
    }

    private static void getOrLoadT1lt1000(@Nonnull final MultiStageCache<String, String, PhysicalPlanEquivalence, String> cache) {
        final var result = cache.reduce("T1", "1", ppeFor(ecFor(1000)), () -> NonnullPair.of(ppeFor(lt1000Constraint),
                generateFullScan()), s -> s + " overriden with 1000", ConcurrentCacheTests::pickFirst, e -> { });
        Assertions.assertThat(result).doesNotContain("150", "500"); // we must not scan index <150 or <500 as the returned results would be incorrect
    }

    @Nonnull
    private static final Random random = new Random();

    private static void randomWorkLoad(@Nonnull final MultiStageCache<String, String, PhysicalPlanEquivalence, String> cache) throws InterruptedException {
        final Map<Integer, Consumer<MultiStageCache<String, String, PhysicalPlanEquivalence, String>>> actions = Map.of(0, ConcurrentCacheTests::getOrLoadT1lt1000,
                1, ConcurrentCacheTests::getOrLoadT1lt300,
                2, ConcurrentCacheTests::getOrLoadT1lt90);
        Thread.sleep(1);
        final var chosen = random.nextInt(3);
        actions.get(chosen).accept(cache);
    }

    @Test
    void cacheWorks() throws InterruptedException {
        final var builder = MultiStageCache.<String, String, PhysicalPlanEquivalence, String>newMultiStageCacheBuilder();
        final var testCache = builder
                .setSize(2)
                .setSecondarySize(2)
                .setTtl(10)
                .setSecondaryTtl(4)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .build();

        ExecutorService service = Executors.newFixedThreadPool(10);
        int numTasks = 100000;
        CountDownLatch latch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            service.submit(() -> {
                try {
                    randomWorkLoad(testCache);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
        }
        latch.await();
    }

}
