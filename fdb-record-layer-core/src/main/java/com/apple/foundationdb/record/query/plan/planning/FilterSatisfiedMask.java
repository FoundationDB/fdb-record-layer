/*
 * FilterSatisfiedMask.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.ComponentWithChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A mask that can be applied over a {@link QueryComponent} to determine whether a filter and any
 * sub-filters or child components have been satisfied. This can be used to track which sub-components
 * of a given filter have already been satisfied by a planned query and which ones might require
 * an additional filter on the returned results.
 */
@API(API.Status.INTERNAL)
public class FilterSatisfiedMask {

    @Nonnull
    private final QueryComponent filter;
    @Nonnull
    private final List<FilterSatisfiedMask> children;
    private boolean satisfied;
    @Nullable
    private KeyExpression expression;

    private FilterSatisfiedMask(@Nonnull QueryComponent filter, @Nonnull List<FilterSatisfiedMask> children, boolean satisfied) {
        this.filter = filter;
        this.children = children;
        this.satisfied = satisfied;
        this.expression = null;
    }

    /**
     * Return whether this filter is known to have been satisfied. This will not
     * this will not traverse down the children of this filter, but it might return
     * <code>true</code> if someone has already performed a traversal and marked
     * this filter as satisfied.
     *
     * @return whether this filter is known to have been satisfied
     */
    public boolean isSatisfied() {
        return satisfied;
    }

    /**
     * Set whether this filter has been satisfied. If set to <code>true</code>, this
     * means that the filter is no longer needed because a query plan has already
     * been able to filter out any records that would match it. Setting it to
     * <code>false</code> means that it might still be needed in some kind of post-filter.
     * In general, users should use {@link #reset()} if they need to say that
     * a filter has not been satisfied, as (unlike this method), that method traverses
     * down the children of this filter and recursively marks them all as
     * unsatisfied.
     *
     * @param satisfied whether this filter has been satisfied already
     */
    public void setSatisfied(boolean satisfied) {
        this.satisfied = satisfied;
    }

    /**
     * Get the expression that satisfied this filter. Setting this expression
     * is optional, and this may return <code>null</code> if the filter
     * has not been satisfied or if the expression was not set.
     *
     * @return the expression that satisfied this filter or <code>null</code>
     */
    @Nullable
    public KeyExpression getExpression() {
        return expression;
    }

    /**
     * Set the expression that satisfied this filter. Users should generally
     * set this to a non-<code>null</code> value, though it should be set to
     * <code>null</code> if the filter is being marked as unsatisfied for
     * whatever reason. However, users should generally call {@link #reset()}
     * if that is the case as that method will recursively reset all
     * of this mask's children as well.
     *
     * @param expression the expression that satisifed this filter
     */
    public void setExpression(@Nullable KeyExpression expression) {
        this.expression = expression;
    }

    /**
     * Get the filter that this mask is masking. In other words, this mask provides
     * an annotated tree over the returned filter tracking with sub-filters have
     * and have not been satisfied.
     *
     * @return the filter this mask is masking
     */
    @Nonnull
    public QueryComponent getFilter() {
        return filter;
    }

    /**
     * Get the list of children of this filter. It might be empty if the
     * child itself does not have any children. The children are returned
     * in the same order as the children of this mask's filter, and the
     * filters of the returned children will be pointer-equal to the children
     * of the mask.
     *
     * @return the list of children of this filter
     */
    @Nonnull
    public List<FilterSatisfiedMask> getChildren() {
        return children;
    }

    /**
     * Return the child mask associated with a child of the mask's filter.
     * In particular, this will check for pointer-equality between the
     * filter provided and all children of this filter. It will then
     * return the mask from the child list that matches. If none is
     * found, it will throw an {@link FilterNotFoundException}.
     *
     * @param childFilter a child of the mask's filter
     * @return the child of this mask that is associated with the given filter
     * @throws FilterNotFoundException if the given filter is not associated with any of the mask's children
     */
    @Nonnull
    public FilterSatisfiedMask getChild(@Nonnull QueryComponent childFilter) {
        for (FilterSatisfiedMask child : children) {
            // Use pointer equality because (1) efficiency and (2) this should
            // only be called by someone who already has a reference to
            // the exact same parent filter.
            if (child.getFilter() == childFilter) {
                return child;
            }
        }
        throw new FilterNotFoundException(childFilter);
    }

    /**
     * Reset this mask so all filters are marked as unsatisfied. This will
     * traverse down any children and reset them as well.
     */
    public void reset() {
        setSatisfied(false);
        setExpression(null);
        if (!children.isEmpty()) {
            children.forEach(FilterSatisfiedMask::reset);
        }
    }

    /**
     * Create a list of filters that are not satisfied. This is mostly for
     * maintaining compatibility with existing code within the planner
     * that expects an unsatisfied filter list rather than an unsatisfied filter tree.
     * This will return an empty list if the filter is satisfied.
     *
     * @return a list of filters that are not satisfied according to this mask
     */
    @Nonnull
    public List<QueryComponent> getUnsatisfiedFilters() {
        if (isSatisfied()) {
            return Collections.emptyList();
        }
        QueryComponent unsatisfiedFilter = getUnsatisfiedFilter();
        if (unsatisfiedFilter == null) {
            return Collections.emptyList();
        } else if (unsatisfiedFilter instanceof AndComponent) {
            return ((AndComponent)unsatisfiedFilter).getChildren();
        } else {
            return Collections.singletonList(unsatisfiedFilter);
        }
    }

    /**
     * Construct a {@link QueryComponent} that is equal to the original {@link QueryComponent}
     * but with any satisfied filters removed. In other words, if one has been able to
     * satisfy some sub-component of this filter through something like an index scan, then
     * that part of the filter will be removed from the filter. If the entire filter has
     * been satisfied, this will return <code>null</code>
     *
     * @return a filter equivalent to the unsatisfied components of the base filter
     */
    @Nullable
    public QueryComponent getUnsatisfiedFilter() {
        if (isSatisfied()) {
            return null;
        }

        if (filter instanceof NestedField) {
            return getUnsatisfiedFilter((NestedField) filter);
        } else if (filter instanceof OneOfThemWithComponent) {
            return getUnsatisfiedFilter((OneOfThemWithComponent) filter);
        } else if (filter instanceof AndComponent) {
            return getUnsatisfiedFilter((AndComponent) filter);
        } else if (filter instanceof OrComponent) {
            return getUnsatisfiedFilter((OrComponent) filter);
        } else {
            // This filter isn't satisfied (it would have been caught earlier)
            // or one of the filters whose satisfied status can be determined by
            // looking at its children, so it must be treated as unsatisfied.
            return filter;
        }
    }

    @Nullable
    private QueryComponent getUnsatisfiedFilter(@Nonnull NestedField filter) {
        if (children.isEmpty()) {
            throw new RecordCoreException("nested filter has no child masks", LogMessageKeys.PARENT_FILTER, filter);
        }
        QueryComponent newChildFilter = children.get(0).getUnsatisfiedFilter();
        if (newChildFilter == null) {
            // The child is satisfied
            setSatisfied(true);
            return null;
        } else if (newChildFilter == filter.getChild()) {
            return filter;
        } else {
            return new NestedField(filter.getFieldName(), newChildFilter);
        }
    }

    @Nullable
    private QueryComponent getUnsatisfiedFilter(@Nonnull OneOfThemWithComponent filter) {
        if (children.isEmpty()) {
            throw new RecordCoreException("nested filter has no child masks", LogMessageKeys.PARENT_FILTER, filter);
        }
        // If the child filter is *completely* satisfied, we can
        // return null, i.e., this filter is satisfied. But if there
        // are any unsatisfied filters, we need to retain the full
        // filter so that we know we are matching the the correct element
        // in the repeated field when checking the unsatisfied filter.
        QueryComponent newChildFilter = children.get(0).getUnsatisfiedFilter();
        if (newChildFilter == null) {
            setSatisfied(true);
            return null;
        } else {
            return filter;
        }
    }

    @Nullable
    private QueryComponent getUnsatisfiedFilter(@Nonnull AndComponent filter) {
        final List<QueryComponent> unsatisfiedChildren = new ArrayList<>(children.size());
        for (FilterSatisfiedMask child : children) {
            QueryComponent newChildFilter = child.getUnsatisfiedFilter();
            if (newChildFilter != null) {
                unsatisfiedChildren.add(newChildFilter);
            }
        }
        if (unsatisfiedChildren.isEmpty()) {
            // All children satisfied.
            setSatisfied(true);
            return null;
        } else if (unsatisfiedChildren.size() == children.size()) {
            // No children satisfied. Return original filter.
            return filter;
        } else if (unsatisfiedChildren.size() == 1) {
            // Only one child. Remove "and" and return just the filter.
            return unsatisfiedChildren.get(0);
        } else {
            // Return an "and" with only what's left.
            return AndComponent.from(unsatisfiedChildren);
        }
    }

    @Nullable
    private QueryComponent getUnsatisfiedFilter(@Nonnull OrComponent filter) {
        boolean allSatisfied = children.stream().map(FilterSatisfiedMask::getUnsatisfiedFilter).allMatch(Objects::isNull);
        if (allSatisfied) {
            setSatisfied(true);
            return null;
        } else {
            return filter;
        }
    }

    /**
     * Determine if the whole filter has been satisfied. If the filter itself has been
     * marked as satisfied, then this will return <code>true</code>. Otherwise, if the filter
     * is a type where if the children have been satisfied, the filter has also been satisfied,
     * then this traverses down the children to see if they have all been satisfied. It will
     * then return <code>true</code> if every child is satisfied and <code>false</code> otherwise.
     * If this filter is unsatisfied and it does not traverse down this mask's children,
     * then it will return <code>false</code>.
     *
     * <p>
     * This is supposed to be functionally equivalent to calling {@link #getUnsatisfiedFilter()}
     * and checking to see if the result is <code>null</code>. This does not attempt to
     * construct the unsatisfied filter if one exists, so this should be cheaper to call.
     * </p>
     *
     * @return whether the whole filter is known to have been satisfied
     */
    public boolean allSatisfied() {
        if (isSatisfied()) {
            return true;
        } else if (filter instanceof AndComponent || filter instanceof NestedField
                   || filter instanceof OneOfThemWithComponent || filter instanceof OrComponent) {
            // These filters are satisfied if all of their children are satisfied
            // Some of them only have one child
            return getChildren().stream().allMatch(FilterSatisfiedMask::allSatisfied);
        } else {
            // The rest, we should not assume they are satisfied because their children are satisfied
            return false;
        }
    }

    /**
     * Merge in another mask over the same filter. This will mark any filter that
     * is satisfied in the other mask as satisfied within this mask. This is useful
     * as it allows the planner to do something like speculatively mark certain fields
     * as satisfied and then only mark them as satisfied in a main mask if the speculative
     * work actually can be used. This will throw a {@link FilterMismatchException} if
     * the mask provided is not a filter over the same filter as this mask. The other
     * mask's filter must be reference-equal to this mask's filter, not just logically equal.
     *
     * @param other another mask over the same filter
     * @throws FilterMismatchException if <code>other</code> is not a mask of the same filter as this mask
     */
    public void mergeWith(@Nonnull FilterSatisfiedMask other) {
        if (other.getFilter() != getFilter()) {
            throw new FilterMismatchException(other.getFilter());
        }
        if (!children.isEmpty()) {
            Iterator<FilterSatisfiedMask> otherChildren = other.getChildren().iterator();
            for (FilterSatisfiedMask child : getChildren()) {
                if (!otherChildren.hasNext()) {
                    throw new RecordCoreException("insufficient children included in other filter mask",
                            LogMessageKeys.FILTER, filter, LogMessageKeys.OTHER_FILTER, other.getFilter());
                }
                child.mergeWith(otherChildren.next());
            }
            if (otherChildren.hasNext()) {
                throw new RecordCoreException("extraneous children included in other filter mask",
                        LogMessageKeys.FILTER, filter, LogMessageKeys.OTHER_FILTER, other.getFilter());
            }
        } else {
            if (!other.getChildren().isEmpty()) {
                throw new RecordCoreException("extraneous children included in other filter mask",
                        LogMessageKeys.FILTER, filter, LogMessageKeys.OTHER_FILTER, other.getFilter());
            }
        }
        if (other.isSatisfied()) {
            setSatisfied(true);
        }
        if (other.getExpression() != null) {
            setExpression(other.getExpression());
        }
    }

    @Override
    public String toString() {
        return "FilterSatisfiedMask(" + getFilter() + ")";
    }

    /**
     * Create a mask around a {@link QueryComponent}. This will traverse down any children of the
     * filter and produce a full tree. It initializes all filters to being unsatisfied.
     *
     * @param filter the filter to create the mask for
     * @return a mask initially claiming this filter and its sub-components are all unsatisfied
     */
    @Nonnull
    public static FilterSatisfiedMask of(@Nonnull QueryComponent filter) {
        // Traverse down the query component tree to produce this mask
        List<FilterSatisfiedMask> children;
        if (filter instanceof ComponentWithSingleChild) {
            children = Collections.singletonList(FilterSatisfiedMask.of(((ComponentWithSingleChild)filter).getChild()));
        } else if (filter instanceof ComponentWithChildren) {
            final List<QueryComponent> filterChildren = ((ComponentWithChildren)filter).getChildren();
            children = new ArrayList<>(filterChildren.size());
            for (QueryComponent childFilter : filterChildren) {
                children.add(FilterSatisfiedMask.of(childFilter));
            }
        } else {
            children = Collections.emptyList();
        }
        return new FilterSatisfiedMask(filter, children, false);
    }

    /**
     * Thrown if the user requests a child mask from a component, but the mask does
     * not contain that component in any of its children.
     */
    public class FilterNotFoundException extends RecordCoreException {
        static final long serialVersionUID = 1L;

        private FilterNotFoundException(@Nonnull QueryComponent childFilter) {
            super("child filter not found", LogMessageKeys.PARENT_FILTER, filter, LogMessageKeys.CHILD_FILTER, childFilter);
        }
    }

    /**
     * Thrown in instances where one expects the underlying filter associated with
     * another <code>FilterSatisfiedMask</code> to match this one but they do not.
     */
    public class FilterMismatchException extends RecordCoreException {
        static final long serialVersionUID = 1L;

        private FilterMismatchException(@Nonnull QueryComponent otherFilter) {
            super("filter from other mask does not match mask filter", LogMessageKeys.FILTER, filter, LogMessageKeys.OTHER_FILTER, otherFilter);
        }
    }
}
