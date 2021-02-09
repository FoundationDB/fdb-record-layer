/*
 * ExpressionRef.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This interface is used mostly as an (admittedly surmountable) barrier to rules mutating bound references directly,
 * which is undefined behavior.
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public interface ExpressionRef<T extends RelationalExpression> extends Bindable, Correlated<ExpressionRef<T>> {
    /**
     * Insert a new expression into this reference.
     * @param newValue the value to be inserted
     * @return {@code true} if the value was not already part of the reference and was inserted, {@code false} if
     *         the given value was already contained in the reference and was therefore not inserted.
     */
    boolean insert(@Nonnull T newValue);

    /**
     * Return the expression contained in this reference. If the reference does not support getting its expresssion
     * (for example, because it holds more than one expression, or none at all), this should throw an exception.
     * @return the expression contained in this reference
     * @throws UngettableReferenceException if the reference does not support retrieving its expression
     */
    @Nonnull
    T get();

    @Nullable
    <U> U acceptPropertyVisitor(@Nonnull PlannerProperty<U> property);

    /**
     * Try to bind the given matcher to this reference. It should not try to match to the members of the reference;
     * if the given matcher needs to match to a {@link RelationalExpression} rather than an {@link ExpressionRef}, it will
     * call {@link #bindWithin(PlannerBindings, ExpressionMatcher)} instead.
     *
     * <p>
     * Binding to references can be a bit subtle: some matchers (such as {@code ReferenceMatcher}) can bind to references
     * directly while others (such as {@code TypeMatcher}) can't, since they need access to the underlying operator
     * which might not even be well defined. This method implements binding to the <em>reference</em>, rather than to
     * its member(s).
     * </p>
     *
     * @param outerBindings preexisting bindings to be used by the matcher
     * @param matcher a matcher to match with
     * @return a stream of bindings if the match succeeded or an empty stream if it failed
     */
    @Nonnull
    @Override
    default Stream<PlannerBindings> bindTo(@Nonnull final PlannerBindings outerBindings, @Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(outerBindings, this, ImmutableList.copyOf(getMembers()));
    }

    /**
     * Try to bind the given matcher to the members of this reference. If this reference has more than one member,
     * it should try to match to any of them and produce a stream of bindings covering all possible combinations.
     * If possible, this should be done in a lazy fashion using the {@link Stream} API, so as to minimize unnecessary
     * work.
     *
     * @param outerBindings preexisting bindings
     * @param matcher an expression matcher to match the member(s) of this reference with
     * @return a stream of bindings if the match succeeded or an empty stream if it failed
     */
    @Nonnull
    Stream<PlannerBindings> bindWithin(@Nonnull PlannerBindings outerBindings, @Nonnull ExpressionMatcher<? extends Bindable> matcher);

    @Nonnull
    <U extends RelationalExpression> ExpressionRef<U> map(@Nonnull Function<T, U> func);

    @Nullable
    <U extends RelationalExpression> ExpressionRef<U> flatMapNullable(@Nonnull Function<T, ExpressionRef<U>> nullableFunc);

    @Nonnull
    ExpressionRef<T> getNewRefWith(@Nonnull T expression);

    boolean containsAllInMemo(@Nonnull ExpressionRef<? extends RelationalExpression> otherRef,
                              @Nonnull AliasMap equivalenceMap);

    RelationalExpressionPointerSet<T> getMembers();

    /**
     * Return all match candidates that partially match this reference. This set must be a subset of all {@link MatchCandidate}s
     * in the {@link PlanContext} during planning. Note that it is possible that a particular match candidate matches this
     * reference more than once.
     * @return a set of match candidates that partially match this reference.
     */
    @Nonnull
    Set<MatchCandidate> getMatchCandidates();

    /**
     * Return all partial matches that match a given expression. This method is agnostic of the {@link MatchCandidate}
     * that created the partial match.
     * @param expression expression to return partial matches for. This expression has to be a member of this reference
     * @return a collection of partial matches that matches the give expression to some candidate
     */
    @Nonnull
    Collection<PartialMatch> getPartialMatchesForExpression(@Nonnull final RelationalExpression expression);

    /**
     * Return all partial matches for the {@link MatchCandidate} passed in.
     * @param candidate match candidate
     * @return a set of partial matches for {@code candidate}
     */
    @Nonnull
    Set<PartialMatch> getPartialMatchesForCandidate(final MatchCandidate candidate);

    /**
     * Add the {@link PartialMatch} passed in to this reference for the {@link MatchCandidate} passed in.
     * @param candidate match candidate this partial match relates to
     * @param partialMatch a new partial match.
     * @return {@code true} if this call added a new partial, {@code false} if the partial match passed in was already
     *         contained in this reference
     */
    boolean addPartialMatchForCandidate(final MatchCandidate candidate, final PartialMatch partialMatch);

    /**
     * An exception thrown when {@link #get()} is called on a reference that does not support it, such as a group reference.
     * A client that encounters this exception is buggy and should not try to recover any information from the reference
     * that threw it.
     *
     * This exceeds the normal maximum inheritance depth because of the depth of the <code>RecordCoreException</code>
     * hierarchy.
     */
    class UngettableReferenceException extends RecordCoreException {
        private static final long serialVersionUID = 1;

        public UngettableReferenceException(String message) {
            super(message);
        }
    }
}
