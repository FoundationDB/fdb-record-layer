/*
 * FilterSatisfiedMaskTest.java
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

import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for the {@link FilterSatisfiedMask} class.
 */
public class FilterSatisfiedMaskTest {

    private void validateMask(@Nonnull QueryComponent filter, @Nonnull List<QueryComponent> children) {
        final FilterSatisfiedMask mask = FilterSatisfiedMask.of(filter);
        assertSame(filter, mask.getFilter());
        assertThat(mask.isSatisfied(), is(false));
        assertNull(mask.getExpression());

        Iterator<QueryComponent> childIterator = children.iterator();
        for (FilterSatisfiedMask childMask : mask.getChildren()) {
            assertSame(childIterator.next(), childMask.getFilter());
        }
        assertThat(childIterator.hasNext(), is(false));
        assertSame(filter, mask.getUnsatisfiedFilter());
    }

    @Test
    public void create() {
        // Scalar field
        final QueryComponent fieldA = Query.field("a").equalsValue("dummy_value");
        validateMask(fieldA, Collections.emptyList());

        // Repeated field
        final QueryComponent oneOfFieldA = Query.field("a").oneOfThem().equalsValue("dummy_value");
        validateMask(oneOfFieldA, Collections.emptyList());

        // Nested field
        final QueryComponent nestedFieldB = Query.field("b").matches(fieldA);
        validateMask(nestedFieldB, Collections.singletonList(fieldA));

        // Nested repeated field
        final QueryComponent oneOfNestedFieldB = Query.field("b").oneOfThem().matches(fieldA);
        validateMask(oneOfNestedFieldB, Collections.singletonList(fieldA));

        // Not component
        final QueryComponent notFieldA = Query.not(fieldA);
        validateMask(notFieldA, Collections.singletonList(fieldA));

        // Or component
        final QueryComponent orFilter = Query.or(notFieldA, oneOfNestedFieldB);
        validateMask(orFilter, Arrays.asList(notFieldA, oneOfNestedFieldB));

        // And component
        final QueryComponent andFilter = Query.and(nestedFieldB, fieldA, orFilter);
        validateMask(andFilter, Arrays.asList(nestedFieldB, fieldA, orFilter));
    }

    @Test
    public void merge() {
        final QueryComponent fieldA = Query.field("a").equalsValue("hello");
        final QueryComponent fieldB = Query.field("b").equalsValue("world");
        final QueryComponent fieldC = Query.field("c").notEquals("!");
        final QueryComponent and = Query.and(fieldA, fieldB, fieldC);
        final FilterSatisfiedMask mask1 = FilterSatisfiedMask.of(and);
        final FilterSatisfiedMask mask2 = FilterSatisfiedMask.of(and);
        mask1.getChild(fieldA).setSatisfied(true);
        mask2.getChild(fieldC).setSatisfied(true);
        mask1.mergeWith(mask2);
        assertSame(fieldB, mask1.getUnsatisfiedFilter());
        assertEquals(Query.and(fieldA, fieldB), mask2.getUnsatisfiedFilter());
    }

    @Test
    public void unsuccessfulMerge() {
        final QueryComponent fieldA = Query.field("a").equalsValue("a");
        final QueryComponent fieldB = Query.field("b").equalsValue("b");
        assertThrows(FilterSatisfiedMask.FilterMismatchException.class,
                () -> FilterSatisfiedMask.of(fieldA).mergeWith(FilterSatisfiedMask.of(fieldB)));
    }

    @Test
    public void getChild() {
        final QueryComponent fieldA = Query.field("a").equalsValue("dummy_value");
        FilterSatisfiedMask fieldAMask = FilterSatisfiedMask.of(fieldA);
        assertThrows(FilterSatisfiedMask.FilterNotFoundException.class,
                () -> fieldAMask.getChild(Query.field("b").equalsValue("blah")));

        final QueryComponent nestedFieldB = Query.field("b").matches(fieldA);
        FilterSatisfiedMask nestedFieldBMask = FilterSatisfiedMask.of(nestedFieldB);
        assertSame(fieldA, nestedFieldBMask.getChild(fieldA).getFilter());
        assertThrows(FilterSatisfiedMask.FilterNotFoundException.class,
                () -> nestedFieldBMask.getChild(Query.field("a").equalsValue("dummy_value")));

        final QueryComponent andComponent = Query.and(nestedFieldB, Query.field("c").equalsValue("something_else"));
        FilterSatisfiedMask andMask = FilterSatisfiedMask.of(andComponent);
        assertSame(nestedFieldB, andMask.getChild(nestedFieldB).getFilter());
        assertThrows(FilterSatisfiedMask.FilterNotFoundException.class,
                () -> andMask.getChild(Query.field("c").equalsValue("something_else")));
    }

    @Test
    public void getUnsatisfiedSimple() {
        final QueryComponent fieldA = Query.field("a").text().containsPhrase("this isn't a real field");
        FilterSatisfiedMask fieldAMask = FilterSatisfiedMask.of(fieldA);
        fieldAMask.setSatisfied(true);
        assertNull(fieldAMask.getUnsatisfiedFilter());
        fieldAMask.reset();
        assertSame(fieldA, fieldAMask.getUnsatisfiedFilter());

        final QueryComponent oneOfFieldA = Query.field("a").oneOfThem().in("$in_param");
        FilterSatisfiedMask oneOfFieldAMask = FilterSatisfiedMask.of(oneOfFieldA);
        oneOfFieldAMask.setSatisfied(true);
        assertNull(oneOfFieldAMask.getUnsatisfiedFilter());
        oneOfFieldAMask.reset();
        assertSame(oneOfFieldA, oneOfFieldAMask.getUnsatisfiedFilter());
    }

    @Test
    public void getUnsatisfiedWithAnd() {
        final QueryComponent fieldA = Query.field("a").text().containsAny("not a real field");
        final QueryComponent fieldB = Query.field("b").lessThan(100);
        final QueryComponent fieldC = Query.field("c").equalsParameter("$fake_param");
        final QueryComponent andFilter = Query.and(fieldA, fieldB, fieldC);
        FilterSatisfiedMask andMask = FilterSatisfiedMask.of(andFilter);
        andMask.getChild(fieldB).setSatisfied(true);
        assertEquals(Query.and(fieldA, fieldC), andMask.getUnsatisfiedFilter());
        andMask.getChild(fieldA).setSatisfied(true);
        assertSame(fieldC, andMask.getUnsatisfiedFilter());
        andMask.getChild(fieldC).setSatisfied(true);
        assertNull(andMask.getUnsatisfiedFilter());
        andMask.reset();
        assertSame(andFilter, andMask.getUnsatisfiedFilter());
    }

    @Test
    public void getUnsatisfiedNested() {
        final QueryComponent fieldA = Query.field("a").isNull();
        final QueryComponent nestedFieldB = Query.field("b").matches(fieldA);
        FilterSatisfiedMask nestedMask = FilterSatisfiedMask.of(nestedFieldB);
        nestedMask.getChild(fieldA).setSatisfied(true);
        assertThat(nestedMask.isSatisfied(), is(false));
        assertNull(nestedMask.getUnsatisfiedFilter());
        assertThat(nestedMask.isSatisfied(), is(true));
        nestedMask.reset();
        assertThat(nestedMask.getChild(fieldA).isSatisfied(), is(false));
        assertSame(nestedFieldB, nestedMask.getUnsatisfiedFilter());

        final QueryComponent oneOfNestedFieldB = Query.field("b").oneOfThem().matches(fieldA);
        FilterSatisfiedMask oneOfNestedMask = FilterSatisfiedMask.of(oneOfNestedFieldB);
        oneOfNestedMask.getChild(fieldA).setSatisfied(true);
        assertThat(oneOfNestedMask.isSatisfied(), is(false));
        assertNull(oneOfNestedMask.getUnsatisfiedFilter());
        assertThat(oneOfNestedMask.isSatisfied(), is(true));
        oneOfNestedMask.reset();
        assertThat(oneOfNestedMask.getChild(fieldA).isSatisfied(), is(false));
        assertSame(oneOfNestedFieldB, oneOfNestedMask.getUnsatisfiedFilter());
    }

    @Test
    public void getUnsatisfiedNestedWithAnd() {
        final QueryComponent fieldA = Query.field("a").lessThanOrEquals(3.14f);
        final QueryComponent fieldB = Query.field("b").isEmpty();
        final QueryComponent fieldC = Query.field("c").startsWith("prefix");
        final QueryComponent andFilter = Query.and(fieldA, fieldB, fieldC);
        final QueryComponent nestedFilter = Query.field("parent").matches(andFilter);
        FilterSatisfiedMask nestedMask = FilterSatisfiedMask.of(nestedFilter);
        nestedMask.getChild(andFilter).getChild(fieldB).setSatisfied(true);
        assertEquals(Query.field("parent").matches(Query.and(fieldA, fieldC)), nestedMask.getUnsatisfiedFilter());
        nestedMask.getChild(andFilter).getChild(fieldA).setSatisfied(true);
        assertEquals(Query.field("parent").matches(fieldC), nestedMask.getUnsatisfiedFilter());
        nestedMask.getChild(andFilter).getChild(fieldC).setSatisfied(true);
        assertNull(nestedMask.getUnsatisfiedFilter());
        nestedMask.reset();
        assertSame(nestedFilter, nestedMask.getUnsatisfiedFilter());

        final QueryComponent oneOfNestedFilter = Query.field("parent").oneOfThem().matches(andFilter);
        FilterSatisfiedMask oneOfNestedMask = FilterSatisfiedMask.of(oneOfNestedFilter);
        oneOfNestedMask.getChild(andFilter).getChild(fieldB).setSatisfied(true);
        assertSame(oneOfNestedFilter, oneOfNestedMask.getUnsatisfiedFilter());
        oneOfNestedMask.getChild(andFilter).getChild(fieldA).setSatisfied(true);
        assertSame(oneOfNestedFilter, oneOfNestedMask.getUnsatisfiedFilter());
        oneOfNestedMask.getChild(andFilter).getChild(fieldC).setSatisfied(true);
        assertNull(oneOfNestedMask.getUnsatisfiedFilter());
        oneOfNestedMask.reset();
        assertSame(oneOfNestedFilter, oneOfNestedMask.getUnsatisfiedFilter());
    }

    @Test
    public void getUnsatisfiedNot() {
        final QueryComponent fieldA = Query.field("a").equalsValue("something");
        final QueryComponent notFilter = Query.not(fieldA);
        FilterSatisfiedMask notMask = FilterSatisfiedMask.of(notFilter);
        notMask.getChild(fieldA).setSatisfied(true);
        assertSame(notFilter, notMask.getUnsatisfiedFilter());
    }

    @Test
    public void getUnsatisfiedOr() {
        final QueryComponent fieldA = Query.field("a").isEmpty();
        final QueryComponent fieldB = Query.field("b").greaterThanOrEquals(2.171828);
        final QueryComponent orFilter = Query.or(fieldA, fieldB);
        FilterSatisfiedMask orMask = FilterSatisfiedMask.of(orFilter);
        orMask.getChild(fieldA).setSatisfied(true);
        assertSame(orFilter, orMask.getUnsatisfiedFilter());
        orMask.getChild(fieldB).setSatisfied(true);
        assertNull(orMask.getUnsatisfiedFilter());
    }

    @Test
    public void getUnsatisfiedList() {
        final QueryComponent fieldA = Query.field("a").notEquals("something");
        final QueryComponent fieldB = Query.field("b").equalsValue("something_else");
        final QueryComponent andFilter = Query.and(fieldA, fieldB);
        FilterSatisfiedMask andMask = FilterSatisfiedMask.of(andFilter);
        assertEquals(Arrays.asList(fieldA, fieldB), andMask.getUnsatisfiedFilters());
        andMask.getChild(fieldA).setSatisfied(true);
        assertEquals(Collections.singletonList(fieldB), andMask.getUnsatisfiedFilters());
        andMask.getChild(fieldB).setSatisfied(true);
        assertEquals(Collections.emptyList(), andMask.getUnsatisfiedFilters());
        andMask.reset();
        andMask.setSatisfied(true);
        assertEquals(Collections.emptyList(), andMask.getUnsatisfiedFilters());

        final QueryComponent nestedField = Query.field("c").matches(fieldA);
        FilterSatisfiedMask nestedMask = FilterSatisfiedMask.of(nestedField);
        assertEquals(Collections.singletonList(nestedField), nestedMask.getUnsatisfiedFilters());
        nestedMask.getChild(fieldA).setSatisfied(true);
        assertEquals(Collections.emptyList(), nestedMask.getUnsatisfiedFilters());

        final QueryComponent nestedAndFilter = Query.field("parent").matches(andFilter);
        FilterSatisfiedMask nestedAndMask = FilterSatisfiedMask.of(nestedAndFilter);
        assertEquals(Collections.singletonList(nestedAndFilter), nestedAndMask.getUnsatisfiedFilters());
        nestedAndMask.getChild(andFilter).getChild(fieldA).setSatisfied(true);
        assertEquals(Collections.singletonList(Query.field("parent").matches(fieldB)), nestedAndMask.getUnsatisfiedFilters());
        nestedAndMask.getChild(andFilter).getChild(fieldB).setSatisfied(true);
        assertEquals(Collections.emptyList(), nestedAndMask.getUnsatisfiedFilters());
    }
}
