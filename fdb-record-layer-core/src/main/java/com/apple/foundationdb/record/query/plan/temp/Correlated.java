/*
 * Correlated.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.google.common.base.Equivalence;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A <em>correlated</em> entity is one which can only be evaluated as a function of some input (usually a {@link Quantifier}
 * from its relational parent).
 * Since a correlated entity can have multiple correlations to similar quantifiers, a correlation is disambiguated by a
 * {@link CorrelationIdentifier}. Correlated entities are said to be correlated to a quantifier {@code q} if the identifiers
 * returned by {@link #getCorrelatedTo()} set contain
 * the correlation identifier that {@code q} uses.
 *
 * Distinguishing the correlation identifier from the identity of the quantifier object simplifies modifications of the
 * graph, for example while applying transformations during query planning.

 * An correlated object can be <em>rebased</em> using {@link #rebase} and a map of translations from one
 * correlation identifier to another, resulting in a new object of type {@code S} that is
 * identical to {@code this} except that all correlated references in the subtree rooted at this entity
 * (i.e., this object's relational children, predicates, etc.) are replaced with their corresponding identifiers
 * per the map.
 *
 * Since translations are associative we allow to rebase many identifiers in the same {@link #rebase} invocation.
 *
 * As the planner DAG objects are immutable, a rebase operation can be expensive as objects have to be recreated
 * as needed. We follow the ideas of other persistent data structures as we do not require a copy to be made.
 * It is perfectly acceptable to return {@code this} if {@code this} is not correlated to any identifier participating
 * in the translation.
 *
 * This interface defines a {@link #semanticEquals} method that also takes an equivalence map, and determines equality up
 * to the equivalences defined therein. Two correlated entities are equivalent even if they use different names, as long
 * as un-bound correlations can be aligned. For example, in:
 *
 * <pre>
 * {@code
 * SELECT *
 * FROM T1 a, T2 b
 * WHERE a.x = b.y
 * UNION (SELECT *
 *        FROM T1 c, T2 d
 *        WHERE c.x = d.y)
 * }
 * </pre>
 *
 * The two SELECT blocks should be considered equal even though they use different correlation names. The
 * {@link #semanticEquals} introduced here considers these blocks equal using a bi-map that allows for the definition
 * of equivalences among correlation identifiers. In this case
 *
 * <pre>
 * {@code
 * (SELECT * FROM T1 a, T2 b WHERE a.x = b.y).equals(SELECT * FROM T1 c, T2 d WHERE c.x = d.y, ImmutableBiMap.of())
 * }
 * </pre>
 *
 * result (through a series of other steps) in an invocation of
 *
 * <pre>
 * {@code
 * (WHERE a.x = b.y).equals(WHERE c.x = d.y, ImmutableBiMap.of(a -> b, c -> d))
 * }
 * </pre>
 *
 * which then results in a {@code true} as {@code a} is considered to be equal to {@code c} and {@code b} is considered
 * to be equal to {@code d}.
 *
 * @param <S> self type of this. Needed for rebasing to a proper constrained at-least type
 */
@API(API.Status.EXPERIMENTAL)
public interface Correlated<S extends Correlated<S>> {
    /**
     * Returns the set of {@link CorrelationIdentifier}s this entity is correlated to.
     * This means that without a process that binds these correlations to values, the {@link RelationalExpression}
     * (or others that implement this interface) cannot even in principle produce a meaningful result. As often times
     * entities that implement this interface use composition and effectively describe trees or DAGs, this method should
     * be the set union of the {@link CorrelationIdentifier}s this object is correlated to as well as all children,
     * constituent parts, etc.
     * @return the set of {@link CorrelationIdentifier}s this entity is correlated to
     */
    @Nonnull
    Set<CorrelationIdentifier> getCorrelatedTo();

    /**
     * Rebases this and all other objects this objects is composed of using a given translation map.
     * @param translationMap a map defining a translation from {@link CorrelationIdentifier}s {@code ids} to
     *        {@link CorrelationIdentifier}s {@code ids'}. After the rebase, every correlation to an {@code id}
     *        contained {@code ids} that is contained or referred to directly or indirectly by {@code this} must have
     *        been transformed to use the mapped counterpart of {@code id} {@code id'} in {@code ids'}. IDs not
     *        contained in the translation map must remain unmodified by the rebase operation.
     * @return a new entity that has been rebased
     */
    @Nonnull
    S rebase(@Nonnull AliasMap translationMap);

    /**
     * Determine equality with respect to an equivalence map between {@link CorrelationIdentifier}s based on
     * semantic equality: equality of the plan fragments implementing under the given correlation bindings.
     * The contract of this variant of {@code equals()} differs from its regular Java counterpart.
     * A correlation is mostly just one part inside of composition of objects that expresses a more complicated
     * (correlated) structure such as a filter. For instance {@code q1.x = 5} uses a correlation to {@code q1}.
     * That entity representing the filter is said to be correlated to {@code q1}. Similarly,
     * {@code q1.x = 6} is a different filter and is not equal under any correlation mapping.
     * In contrast, consider a predicate where everything except the correlation is the same, such as {@code q2.x = 5}.
     * Without a binding, these two filters are not the same. However, the filters may be a part of some other entity
     * that can express correlations:
     * {@code SELECT * FROM T as q1 WHERE q1.x = 5} is equal to
     * {@code SELECT * FROM T as q2 WHERE q2.x = 5}. It does not matter that {@code q1} and {@code q2} are named
     * differently. It is important, however, that their semantic meaning is the same. In the example both {@code q1}
     * and {@code q2} refer to a record type {@code T} which presumably is the same record type. Therefore, these two
     * query blocks <em>are</em> the same, even though they are labelled differently.
     * In the context of this method, we can establish equality between {@code q1.x = 5} and {@code q2.x = 5} if
     * we know that {@code q1} and {@code q2} refer to the same underlying entity. The equivalence map passed in
     * encodes that equality between {@link CorrelationIdentifier}s.
     *
     * Note: This method has the same interaction with {@link Object#hashCode()} as the regular {@code equals()}
     *       method. As we can only ever establish true equality using equivalence maps, {@code hashCode()}
     *       implementations in implementors of this interface <em>must not</em> be dependent on any correlation identifiers
     *       used in the structure of the entity. Doing so might violate the fundamental property of hash codes:
     *       {@code e1.equals(e2, ...) => e1.hashCode() == e2.hashCode()}
     *
     * @param other the other object to establish equality with
     * @param aliasMap a map of {@link CorrelationIdentifier}s {@code ids} to {@code ids'}. A correlation
     *        identifier {@code id} used in {@code this} should be considered equal to another correlation identifier
     *        {@code id'} used in {@code other} if either they are the same by {@link Object#equals}
     *        of if there is a mapping from {@code id} to {@code id'}.
     * @return {@code true} if both entities are considered equal using the equivalences passed in, {@code false}
     *         otherwise
     */
    boolean semanticEquals(@Nullable Object other, @Nonnull AliasMap aliasMap);

    /**
     * Return a semantic hash code for this object. The hash code must obey the convention that for any two objects
     * {@code o1} and {@code o2} and for every {@link AliasMap} {@code aliasMap}:
     *
     * <pre>
     * {@code o1.semanticEquals(o1, aliasMap)} follows {@code o1.semanticHash() == o2.semanticHash()}
     * </pre>
     *
     * If the semantic hash code of two implementing objects is equal and {@link #semanticEquals} returns {@code true}
     * for these two objects the plan fragments are considered to produce the same result under the given
     * correlations bindings.
     *
     * The left side {@code o1.semanticEquals(o1, aliasMap)} depends on an {@link AliasMap} while
     * {@code o1.semanticHash() == o2.semanticHash()} does not. Hence, the hash that is computed is identical regardless
     * of possible bindings.
     *
     * @return the semantic hash code
     */
    int semanticHashCode();

    /**
     * Helper class to wrap extenders of {@link Correlated} to provide standard equivalence and hash code
     * functionality under a given {@link AliasMap}.
     */
    class BoundEquivalence extends Equivalence<Correlated<?>> {
        @Nonnull
        private final AliasMap aliasMap;

        public BoundEquivalence(@Nonnull final AliasMap aliasMap) {
            this.aliasMap = aliasMap;
        }

        @Override
        protected boolean doEquivalent(final Correlated<?> a, final Correlated<?> b) {
            return a.semanticEquals(b, aliasMap);
        }

        @Override
        protected int doHash(final Correlated<?> correlated) {
            return correlated.semanticHashCode();
        }
    }
}
