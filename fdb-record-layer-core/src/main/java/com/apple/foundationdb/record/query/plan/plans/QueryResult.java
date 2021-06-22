/*
 * QueryResult.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * QueryResult is the general result that encapsulates the data that is flowing up from plan to consumer. The QueryResult
 * can hold several elements for each result. The elements are opaque from the result perspective, but are known at planning time
 * so that the planner can address them as needed and extract and transfer them from one cursor to another.
 * This class is immutable - all modify operations will cause a new instance to be created with the modified value, leaving
 * the original instance intact. The internal data structure is also immutable.
 */
@API(API.Status.UNSTABLE)
public class QueryResult {
    @Nonnull
    private final List<QueryResultElement> elements;

    private static final QueryResult EMPTY = new QueryResult(Collections.emptyList());

    private QueryResult(@Nonnull List<QueryResultElement> elements) {
        this.elements = elements;
    }

    /**
     * Get an empty result.
     * @return An immutable empty result.
     */
    public QueryResult empty() {
        return EMPTY;
    }

    /**
     * Create a new result with the given element.
     * @param element the given element
     * @return the newly created result
     */
    @Nonnull
    public static QueryResult of(@Nullable QueryResultElement element) {
        return new QueryResult(Collections.singletonList(element));
    }

    /**
     * Create a new result with the given elements.
     * @param elements the collection of elements to populate in the result
     * @return the newly created result
     */
    public static QueryResult of(Collection<QueryResultElement> elements) {
        return new QueryResult(ImmutableList.copyOf(elements));
    }

    /**
     * Create a new result that extends the current result with an additional element.
     * @param addedElement the element to add to the result
     * @return the newly created result that combines the existing elements with the new one
     */
    public QueryResult with(QueryResultElement addedElement) {
        return new QueryResult(new ImmutableList.Builder<QueryResultElement>().addAll(elements).add(addedElement).build());
    }

    /**
     * Create a new result that extends the current result with additional elements.
     * @param addedElements the elements to add to the result
     * @return the newly created result that combines the existing elements with the new ones
     */
    public QueryResult with(QueryResultElement... addedElements) {
        return new QueryResult(new ImmutableList.Builder<QueryResultElement>().addAll(elements).add(addedElements).build());
    }

    /**
     * Retrieve the element at the ith position in the result (0 based).
     * @param i the index of the requested element.
     * @return the required element
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    @Nullable
    public QueryResultElement get(int i) {
        return elements.get(i);
    }

    /**
     * FDBQueriedRecord compatibility method. Return the element in a position, assuming that it is a {@link FDBQueriedRecord}
     * @param i the index of the requested element.
     * @param <M> the type of record the store is providing (for the {@link FDBQueriedRecord} compatibility)
     * @return the element, in case it is of the right type
     * @throws ClassCastException in case the element is of the wrong type
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    @Nullable
    @SuppressWarnings("unchecked") // Intend to throw ClassCast in case the element is of teh wrong type
    public <M extends Message> FDBQueriedRecord<M> getQueriedRecord(int i) {
        return ((FDBQueriedRecord<M>)elements.get(i));
    }

    /**
     * FDBQueriedRecord compatibility method. Return the stored record from the element in a position, assuming that it is a {@link FDBQueriedRecord}
     * @param i the index of the requested element.
     * @return the element, in case it is of the right type
     * @throws ClassCastException in case the element is of the wrong type
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    @Nullable
    public FDBStoredRecord<Message> getStoredRecord(int i) {
        FDBQueriedRecord<Message> record = getQueriedRecord(i);
        return (record == null) ? null : record.getStoredRecord();
    }

    /**
     * FDBQueriedRecord compatibility method. Return the index from the element in a position, assuming that it is a {@link FDBQueriedRecord}
     * @param i the index of the requested element.
     * @return the element, in case it is of the right type
     * @throws ClassCastException in case the element is of the wrong type
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    @Nullable
    public Index getIndex(int i) {
        FDBQueriedRecord<Message> record = getQueriedRecord(i);
        return (record == null) ? null : record.getIndex();
    }

    /**
     * FDBQueriedRecord compatibility method. Return the indexEntry from the element in a position, assuming that it is a {@link FDBQueriedRecord}
     * @param i the index of the requested element.
     * @return the element, in case it is of the right type
     * @throws ClassCastException in case the element is of the wrong type
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    @Nullable
    public IndexEntry getIndexEntry(int i) {
        FDBQueriedRecord<Message> record = getQueriedRecord(i);
        return (record == null) ? null : record.getIndexEntry();
    }
}
