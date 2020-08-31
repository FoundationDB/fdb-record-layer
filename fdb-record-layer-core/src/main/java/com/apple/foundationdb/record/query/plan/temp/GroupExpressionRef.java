/*
 * GroupExpressionRef.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A Cascades-style group expression, representing the members of set of {@link RelationalExpression}s that belong to
 * the same equivalence class.
 *
 * <p>
 * The <em>memo</em> data structure can compactly represent a large set of similar {@link RelationalExpression}s through
 * careful memoization. The Cascades "group expression", represented by the {@code GroupExpressionRef}, is the key to
 * that memoization by sharing optimization work on a sub-expression with other parts of the expression that reference
 * the same sub-expression.
 * </p>
 *
 * <p>
 * The reference abstraction is designed to make it difficult for authors of rules to mutate group expressions directly,
 * which is undefined behavior. Note that a {@link GroupExpressionRef} cannot be "dereferenced" using the {@link #get()}
 * method if it contains more than one member. Expressions with more than one member should not be used outside of the
 * query planner, and {@link #get()} should not be used inside the query planner.
 * </p>
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public class GroupExpressionRef<T extends RelationalExpression> implements ExpressionRef<T> {
    static final GroupExpressionRef<RelationalExpression> EMPTY = new GroupExpressionRef<>();

    @Nonnull
    private final RelationalExpressionPointerSet<T> members;
    private boolean explored = false;

    public GroupExpressionRef() {
        members = new RelationalExpressionPointerSet<>();
    }

    protected GroupExpressionRef(@Nonnull T expression) {
        members = new RelationalExpressionPointerSet<>();
        members.add(expression);
    }

    private GroupExpressionRef(@Nonnull RelationalExpressionPointerSet<T> members) {
        this.members =  members;
    }

    @Nonnull
    @Override
    public T get() {
        if (members.size() == 1) {
            return members.iterator().next();
        }
        throw new UngettableReferenceException("tried to dereference GroupExpressionRef with " + members.size() + " members");
    }

    public synchronized boolean replace(@Nonnull T newValue) {
        clear();
        return insert(newValue);
    }

    @Override
    public boolean insert(@Nonnull T newValue) {
        if (!containsInMemo(newValue)) {
            members.add(newValue);
            return true;
        }
        return false;
    }
    
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean containsExactly(@Nonnull T expression) {
        return members.contains(expression);
    }

    @Override
    public boolean containsAllInMemo(@Nonnull final ExpressionRef<? extends RelationalExpression> otherRef,
                                     @Nonnull final AliasMap equivalenceMap) {
        for (final RelationalExpression otherMember : otherRef.getMembers()) {
            if (!containsInMemo(otherMember, equivalenceMap)) {
                return false;
            }
        }
        return true;
    }

    public boolean containsInMemo(@Nonnull final RelationalExpression expression) {
        final Set<CorrelationIdentifier> correlatedTo = getCorrelatedTo();
        final Set<CorrelationIdentifier> otherCorrelatedTo = expression.getCorrelatedTo();

        final Sets.SetView<CorrelationIdentifier> commonUnbound = Sets.intersection(correlatedTo, otherCorrelatedTo);
        final AliasMap identityMap = AliasMap.identitiesFor(commonUnbound);

        return containsInMemo(expression, identityMap);
    }

    private boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                   @Nonnull final AliasMap equivalenceMap) {
        for (final RelationalExpression member : members) {
            if (containsInMember(member, expression, equivalenceMap)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsInMember(@Nonnull RelationalExpression member,
                                            @Nonnull RelationalExpression otherMember,
                                            @Nonnull final AliasMap equivalenceMap) {
        if (member.getClass() != otherMember.getClass()) {
            return false;
        }

        // We know member and otherMember are of the same class. canCorrelate() needs to match as well.
        Verify.verify(member.canCorrelate() == otherMember.canCorrelate());

        final Iterable<AliasMap> quantifierMapIterable =
                Quantifiers.match(
                        member.getQuantifiers(),
                        otherMember.getQuantifiers(),
                        member.canCorrelate(),
                        equivalenceMap,
                        ((quantifier, otherQuantifier, nestedEquivalencesMap) -> {
                            final ExpressionRef<? extends RelationalExpression> rangesOver = quantifier.getRangesOver();
                            final ExpressionRef<? extends RelationalExpression> otherRangesOver = otherQuantifier.getRangesOver();
                            return rangesOver.containsAllInMemo(otherRangesOver, nestedEquivalencesMap);
                        }));

        return StreamSupport.stream(quantifierMapIterable.spliterator(), false)
                .anyMatch(quantifierMap -> member.equalsWithoutChildren(otherMember,
                        equivalenceMap.compose(quantifierMap)));
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        for (final T member : getMembers()) {
            builder.addAll(member.getCorrelatedTo());
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public GroupExpressionRef<T> rebase(@Nonnull final AliasMap translationMap) {
        return GroupExpressionRef.from(getMembers()
                .stream()
                // The following downcast is necessary since members of this class are of type T
                // (extends RelationalExpression) but rebases of RelationalExpression are not
                // Rebaseable of T (extends RelationalExpression) but Correlated<RelationalExpression> in order to
                // avoid introducing a new type Parameter T. All the o1 = o.rebase(), however, by contract should return
                // an o1 where o1.getClass() == o.getClass()
                .map(member -> (T)member.rebase(translationMap))
                .collect(Collectors.toList()));
    }

    public void clear() {
        members.clear();
    }

    public void setExplored() {
        this.explored = true;
    }

    public boolean isExplored() {
        return explored;
    }

    @Nonnull
    @Override
    public RelationalExpressionPointerSet<T> getMembers() {
        return members;
    }

    @Nonnull
    @Override
    public ExpressionRef<T> getNewRefWith(@Nonnull T expression) {
        return new GroupExpressionRef<>(expression);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindWithin(@Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        Stream.Builder<Stream<PlannerBindings>> memberStreams = Stream.builder();
        for (T member : members) {
            memberStreams.add(member.bindTo(matcher));
        }
        return memberStreams.build().flatMap(Function.identity()); // concat
    }

    @Nullable
    @Override
    public <U> U acceptPropertyVisitor(@Nonnull PlannerProperty<U> property) {
        if (property.shouldVisit(this)) {
            final List<U> memberResults = new ArrayList<>(members.size());
            for (T member : members) {
                final U result = member.acceptPropertyVisitor(property);
                if (result == null) {
                    return null;
                }
                memberResults.add(result);
            }
            return property.evaluateAtRef(this, memberResults);
        }
        return null;
    }

    @Nonnull
    @Override
    public <U extends RelationalExpression> ExpressionRef<U> map(@Nonnull Function<T, U> func) {
        RelationalExpressionPointerSet<U> resultMembers = new RelationalExpressionPointerSet<>();
        members.iterator().forEachRemaining(member -> resultMembers.add(func.apply(member)));
        return new GroupExpressionRef<>(resultMembers);
    }

    @Nullable
    @Override
    public <U extends RelationalExpression> ExpressionRef<U> flatMapNullable(@Nonnull Function<T, ExpressionRef<U>> nullableFunc) {
        RelationalExpressionPointerSet<U> mappedMembers = new RelationalExpressionPointerSet<>();
        for (T member : members) {
            ExpressionRef<U> mapped = nullableFunc.apply(member);
            if (mapped instanceof GroupExpressionRef) {
                mappedMembers.addAll(((GroupExpressionRef<U>)mapped).members);
            }
        }
        return new GroupExpressionRef<>(mappedMembers);
    }

    @Override
    public String toString() {
        return "ExpressionRef@" + hashCode() + "(" + "explored=" + explored + ")";
    }

    public static <T extends RelationalExpression> GroupExpressionRef<T> of(@Nonnull T expression) {
        return new GroupExpressionRef<>(expression);
    }

    @SuppressWarnings("unchecked")
    public static <T extends RelationalExpression> GroupExpressionRef<T> of(@Nonnull T... expressions) {
        return from(Arrays.asList(expressions));
    }

    public static <T extends RelationalExpression> GroupExpressionRef<T> from(@Nonnull Collection<T> expressions) {
        RelationalExpressionPointerSet<T> members = new RelationalExpressionPointerSet<>();
        members.addAll(expressions);
        return new GroupExpressionRef<>(members);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GroupExpressionRef<T> otherRef = (GroupExpressionRef<T>)other;

        final Iterator<T> iterator = members.iterator();
        final ImmutableMultimap.Builder<Integer, T> expressionsMapBuilder = ImmutableMultimap.builder();

        while (iterator.hasNext()) {
            final T next = iterator.next();
            expressionsMapBuilder.put(next.semanticHashCode(), next);
        }

        final ImmutableMultimap<Integer, T> expressionsMap = expressionsMapBuilder.build();

        for (final T otherMember : otherRef.getMembers()) {
            if (expressionsMap.get(otherMember.semanticHashCode()).stream().anyMatch(member -> member.semanticEquals(otherMember, equivalenceMap))) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int semanticHashCode() {
        final Iterator<T> iterator = members.iterator();
        final ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();

        while (iterator.hasNext()) {
            final T next = iterator.next();
            builder.add(next.semanticHashCode());
        }

        return Objects.hash(builder.build());
    }
}
