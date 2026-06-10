/*
 * ConcurrentTransactionHarness.java
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Fluent harness for exercising concurrent {@link FDBRecordContext} transactions in a test.
 * Opens N overlapping contexts, runs per-context actions in declared order, then commits in
 * declared order (or shuffled), capturing per-tx commit outcomes for assertion-style checks.
 *
 * <p>Typical usage (statically import {@link #concurrent}):</p>
 * <pre>{@code
 * concurrent(this::openContext, ctx -> openTestStore(ctx))
 *         .tx("A", () -> doSomething())
 *         .tx("B", () -> doSomethingElse())
 *         .commitAll()
 *         .expectNoConflicts();
 * }</pre>
 */
public final class ConcurrentTransactionHarness {

    private ConcurrentTransactionHarness() {
    }

    /**
     * Entry point for the fluent concurrent-transaction harness. Opens N contexts, binds
     * each one via {@code storeOpener} (typically the test's {@code openStore(ctx, ...)}),
     * runs per-context actions, and commits in declared order.
     *
     * @param contextSupplier opens a fresh {@link FDBRecordContext} for each registered
     *                        transaction (typically {@code testBase::openContext})
     * @param storeOpener binds the test's {@code recordStore} field to the given context
     *                    (typically {@code ctx -> openStore(ctx, ...)})
     */
    @Nonnull
    public static ConcurrentScenario concurrent(@Nonnull final Supplier<FDBRecordContext> contextSupplier,
                                                @Nonnull final ContextSetup storeOpener) {
        return new ConcurrentScenario(contextSupplier, storeOpener);
    }

    /**
     * Binds a context to the test's record store (e.g. by calling the test's
     * {@code openStore(ctx, ...)}). Invoked once per registered transaction before its
     * action runs.
     */
    @FunctionalInterface
    public interface ContextSetup {
        void setup(@Nonnull FDBRecordContext context) throws Exception;
    }

    /**
     * Action run inside a transaction. The test's {@code recordStore} is bound to the
     * matching context before the action runs, so callers can use the test's normal
     * helpers without thinking about which context is active.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * A scenario builder. Each {@link #tx(String, ThrowingRunnable)} call registers a
     * named transaction; {@link #commitAll()} opens all contexts, runs the actions in
     * declared order against their respective stores, then commits each context in
     * declared order — capturing per-tx commit outcomes for later assertions.
     */
    public static final class ConcurrentScenario {
        @Nonnull
        private final Supplier<FDBRecordContext> contextSupplier;
        @Nonnull
        private final ContextSetup storeOpener;
        @Nonnull
        private final List<NamedTx> transactions = new ArrayList<>();

        ConcurrentScenario(@Nonnull final Supplier<FDBRecordContext> contextSupplier,
                           @Nonnull final ContextSetup storeOpener) {
            this.contextSupplier = contextSupplier;
            this.storeOpener = storeOpener;
        }

        /**
         * Registers a named transaction. The {@code action} runs against a freshly
         * opened context whose store has been wired up via the scenario's store opener.
         */
        @Nonnull
        public ConcurrentScenario tx(@Nonnull final String name, @Nonnull final ThrowingRunnable action) {
            transactions.add(new NamedTx(name, action));
            return this;
        }

        /**
         * Opens all registered contexts (so they overlap), runs each action with its
         * context's store bound, then commits each context in declared order. Commit
         * failures are captured per-tx rather than bubbled out, so subsequent commits
         * still attempt to run and the resulting {@link CommitOutcome} can describe the
         * full picture.
         */
        @Nonnull
        public CommitOutcome commitAll() {
            return runAndCommit(transactions);
        }

        /**
         * Same as {@link #commitAll()} but shuffles the commit order using the given
         * {@link Random}. Useful when the test asserts an outcome that should hold
         * regardless of which transaction commits first; pair with
         * {@code @ParameterizedTest @RandomSeedSource} so the test can vary the seed
         * across runs while still being reproducible from a logged seed on failure.
         */
        @Nonnull
        public CommitOutcome commitInAnyOrder(@Nonnull final Random random) {
            final List<NamedTx> shuffled = new ArrayList<>(transactions);
            Collections.shuffle(shuffled, random);
            return runAndCommit(shuffled);
        }

        @Nonnull
        private CommitOutcome runAndCommit(@Nonnull final List<NamedTx> commitOrder) {
            for (NamedTx tx : transactions) {
                tx.context = contextSupplier.get();
                tx.context.getReadVersion();
            }
            try {
                for (NamedTx tx : transactions) {
                    storeOpener.setup(tx.context);
                    tx.action.run();
                }
                for (NamedTx tx : commitOrder) {
                    try {
                        tx.context.commit();
                    } catch (Throwable t) {
                        tx.commitError = t;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("scenario action failed before commit phase", e);
            } finally {
                for (NamedTx tx : transactions) {
                    if (tx.context != null) {
                        tx.context.close();
                    }
                }
            }
            return new CommitOutcome(transactions);
        }
    }

    /**
     * Result of {@link ConcurrentScenario#commitAll()}. Provides expectation-style
     * assertions over the per-tx commit outcomes.
     */
    public static final class CommitOutcome {
        @Nonnull
        private final List<NamedTx> transactions;

        CommitOutcome(@Nonnull final List<NamedTx> transactions) {
            this.transactions = transactions;
        }

        /**
         * Asserts that every registered transaction committed successfully (no
         * conflicts, no other commit-time failures).
         */
        @Nonnull
        public CommitOutcome expectNoConflicts() {
            for (NamedTx tx : transactions) {
                assertNull(tx.commitError,
                        "Expected no commit conflicts but tx '" + tx.name + "' failed: " + tx.commitError);
            }
            return this;
        }

        /**
         * Asserts that the named transaction failed to commit with a transaction
         * conflict. Other transactions are not checked.
         */
        @Nonnull
        public CommitOutcome expectConflictOn(@Nonnull final String txName) {
            final NamedTx tx = find(txName);
            assertNotNull(tx.commitError,
                    "Expected tx '" + txName + "' to fail commit, but it committed successfully");
            assertInstanceOf(FDBExceptions.FDBStoreTransactionConflictException.class, tx.commitError,
                    "Expected tx '" + txName + "' to fail with a transaction conflict, but failed with: " + tx.commitError);
            return this;
        }

        /**
         * Asserts that the named transaction committed successfully.
         */
        @Nonnull
        public CommitOutcome expectCommitted(@Nonnull final String txName) {
            final NamedTx tx = find(txName);
            assertNull(tx.commitError,
                    "Expected tx '" + txName + "' to commit, but it failed: " + tx.commitError);
            return this;
        }

        @Nonnull
        private NamedTx find(@Nonnull final String txName) {
            return transactions.stream()
                    .filter(t -> t.name.equals(txName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Unknown tx: " + txName));
        }
    }

    private static final class NamedTx {
        @Nonnull final String name;
        @Nonnull final ThrowingRunnable action;
        @Nullable FDBRecordContext context;
        @Nullable Throwable commitError;

        NamedTx(@Nonnull final String name, @Nonnull final ThrowingRunnable action) {
            this.name = name;
            this.action = action;
        }
    }
}
