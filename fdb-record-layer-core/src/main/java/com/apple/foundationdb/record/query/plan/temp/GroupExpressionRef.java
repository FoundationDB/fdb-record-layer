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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A Cascades-style group expression, representing the members of set of {@link PlannerExpression}s that belong to
 * the same equivalence class.
 *
 * <p>
 * The <em>memo</em> data structure can compactly represent a large set of similar {@link PlannerExpression}s through
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
public class GroupExpressionRef<T extends PlannerExpression> implements MutableExpressionRef<T> {
    static final GroupExpressionRef<PlannerExpression> EMPTY = new GroupExpressionRef<>();

    @Nonnull
    private final PlannerExpressionPointerSet<T> members;
    private boolean explored = false;

    public GroupExpressionRef() {
        members = new PlannerExpressionPointerSet<>();
    }

    protected GroupExpressionRef(@Nonnull T expression) {
        members = new PlannerExpressionPointerSet<>();
        members.add(expression);
    }

    private GroupExpressionRef(@Nonnull PlannerExpressionPointerSet<T> members) {
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

    @Override
    public void insert(@Nonnull T newValue) {
        if (!containsInMemo(newValue)) {
            members.add(newValue);
        }
    }

    public void insertAll(@Nonnull GroupExpressionRef<T> newValues) {
        for (T member : newValues.members) {
            insert(member);
        }
    }

    public void removeMemberIfPresent(@Nonnull T member) {
        members.remove(member);
    }

    public void removeMember(@Nonnull T member) {
        if (!members.remove(member)) {
            throw new RecordCoreArgumentException("tried to remove member that isn't present")
                    .addLogInfo("member", member);
        }
    }

    public boolean containsExactly(@Nonnull T expression) {
        return members.contains(expression);
    }

    @Override
    public boolean containsAllInMemo(@Nonnull ExpressionRef<? extends PlannerExpression> otherRef) {
        for (PlannerExpression otherMember : otherRef.getMembers()) {
            if (!containsInMemo(otherMember)) {
                return false;
            }
        }
        return true;
    }

    public boolean containsInMemo(@Nonnull PlannerExpression expression) {
        for (PlannerExpression member : members) {
            if (containsInMember(member, expression)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsInMember(@Nonnull PlannerExpression member, @Nonnull PlannerExpression otherMember) {
        if (!member.equalsWithoutChildren(otherMember)) {
            return false;
        }

        Iterator<? extends ExpressionRef<? extends PlannerExpression>> memberChildren = member.getPlannerExpressionChildren();
        Iterator<? extends ExpressionRef<? extends PlannerExpression>> otherMemberChildren = otherMember.getPlannerExpressionChildren();
        while (memberChildren.hasNext() && otherMemberChildren.hasNext()) {
            if (!memberChildren.next().containsAllInMemo(otherMemberChildren.next())) {
                return false;
            }
        }
        return !memberChildren.hasNext() && !otherMemberChildren.hasNext();
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
    public PlannerExpressionPointerSet<T> getMembers() {
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
    public <U extends PlannerExpression> ExpressionRef<U> map(@Nonnull Function<T, U> func) {
        PlannerExpressionPointerSet<U> resultMembers = new PlannerExpressionPointerSet<>();
        members.iterator().forEachRemaining(member -> resultMembers.add(func.apply(member)));
        return new GroupExpressionRef<U>(resultMembers);
    }

    @Nullable
    @Override
    public <U extends PlannerExpression> ExpressionRef<U> flatMapNullable(@Nonnull Function<T, ExpressionRef<U>> nullableFunc) {
        PlannerExpressionPointerSet<U> mappedMembers = new PlannerExpressionPointerSet<>();
        for (T member : members) {
            ExpressionRef<U> mapped = nullableFunc.apply(member);
            if (mapped instanceof GroupExpressionRef) {
                mappedMembers.addAll(((GroupExpressionRef<U>)mapped).members);
            }
        }
        return new GroupExpressionRef<>(mappedMembers);
    }

    public static <T extends PlannerExpression> GroupExpressionRef<T> of(@Nonnull T expression) {
        return new GroupExpressionRef<>(expression);
    }

    @SuppressWarnings("unchecked")
    public static <T extends PlannerExpression> GroupExpressionRef<T> of(@Nonnull T... expressions) {
        return from(Arrays.asList(expressions));
    }

    public static <T extends PlannerExpression> GroupExpressionRef<T> from(@Nonnull Collection<T> expressions) {
        PlannerExpressionPointerSet<T> members = new PlannerExpressionPointerSet<>();
        for (T expression : expressions) {
            members.add(expression);
        }
        return new GroupExpressionRef<>(members);
    }
}
