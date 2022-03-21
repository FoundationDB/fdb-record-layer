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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * This interface is used mostly as an (admittedly surmountable) barrier to rules mutating bound references directly,
 * which is undefined behavior.
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public interface ExpressionRef<T extends RelationalExpression> extends Correlated<ExpressionRef<T>>, Typed {
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

    boolean containsAllInMemo(@Nonnull ExpressionRef<? extends RelationalExpression> otherRef,
                              @Nonnull AliasMap equivalenceMap);

    LinkedIdentitySet<T> getMembers();

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
    @SuppressWarnings("UnusedReturnValue")
    boolean addPartialMatchForCandidate(final MatchCandidate candidate, final PartialMatch partialMatch);

    @Nonnull
    InterestingPropertiesMap getRequirementsMap();

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
