/*
 * QueryPredicateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the {@link QueryPredicate} implementations.
 */
public class QueryPredicateTest {
    private Boolean evaluate(@Nonnull QueryPredicate predicate) {
        return evaluate(predicate, Bindings.EMPTY_BINDINGS);
    }

    private Boolean evaluate(@Nonnull QueryPredicate predicate, @Nonnull Bindings bindings) {
        return predicate.eval(null, EvaluationContext.forBindings(bindings), null, null);
    }

    private QueryPredicate and(@Nonnull QueryPredicate... predicates) {
        return new AndPredicate(ImmutableList.copyOf(predicates));
    }

    private QueryPredicate or(@Nonnull QueryPredicate... predicates) {
        return new OrPredicate(ImmutableList.copyOf(predicates));
    }

    private abstract static class TestPredicate implements QueryPredicate {
        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 0;
        }

        @Nonnull
        @Override
        public Stream<PlannerBindings> bindTo(@Nonnull final PlannerBindings outerBindings, @Nonnull ExpressionMatcher<? extends Bindable> matcher) {
            return Stream.empty();
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public TestPredicate rebase(@Nonnull final AliasMap translationMap) {
            // TestPredicate is immutable
            return this;
        }

        @Override
        public boolean equals(final Object other) {
            return semanticEquals(other, AliasMap.emptyMap());
        }

        @Override
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return this == other;
        }

        @Override
        public int hashCode() {
            return semanticHashCode();
        }

        @Override
        public int semanticHashCode() {
            return 31;
        }
    }

    private static final QueryPredicate TRUE = new TestPredicate() {
        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            return Boolean.TRUE;
        }
    };

    private static final QueryPredicate FALSE = new TestPredicate() {
        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            return Boolean.FALSE;
        }
    };

    private static final QueryPredicate NULL = new TestPredicate() {
        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            return null;
        }
    };

    @Test
    public void testAnd() throws Exception {
        assertNull(evaluate(and(TRUE, NULL)));
        // Use equals here, because assertTrue/False would throw nullPointerException if and() returns null
        assertEquals(true, evaluate(and(TRUE, TRUE)));
        assertEquals(false, evaluate(and(TRUE, FALSE)));
        assertEquals(false, evaluate(and(NULL, FALSE)));
        assertNull(evaluate(and(NULL, TRUE)));
    }

    @Test
    public void testOr() throws Exception {
        final ExpressionTestsProto.TestScalarFieldAccess val = ExpressionTestsProto.TestScalarFieldAccess.newBuilder().build();
        assertNull(evaluate(or(FALSE, NULL)));
        // Use equals here, because assertTrue/False would throw nullPointerException if or() returns null
        assertEquals(true, evaluate(or(FALSE, TRUE)));
        assertEquals(true, evaluate(or(TRUE, FALSE)));
        assertEquals(false, evaluate(or(FALSE, FALSE)));
        assertEquals(true, evaluate(or(NULL, TRUE)));
    }
}
