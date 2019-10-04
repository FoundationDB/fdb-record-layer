/*
 * Source.java
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.annotation.API;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A source represents a stream of entities that can be used to generate {@link Element}s, such as records or field values.
 *
 * <p>
 * Sources can stand alone or be derived from other sources. By combining sources, complex streams of values derived
 * from stored information can be described and manipulated. For example:
 * </p>
 * <ul>
 *     <li>A {@link RecordTypeSource} represents the stream of all records of a particular record type.</li>
 *     <li>A {@link RepeatedFieldSource} represents the stream of all (nested) values in a repeated field of a record.</li>
 * </ul>
 * <p>
 * Generally, a query or {@link ViewExpression} has a "root" source. This root source is not derived from another source
 * and defines a stream of entries from which all other sources are derived.
 * </p>
 *
 * <p>
 * A source encapsulates the logic for generating and combining streams of data. For example, if there are two
 * {@code RepeatedFieldSource}s derived from the same {@code RecordTypeSource}, the source encapsulates the implicit
 * Cartesian product between the two. Similarly, it encapsulates the logic for checking whether or not a particular
 * source has already been "used up" by the query planner.
 * </p>
 *
 * @see Element
 * @see ViewExpression
 */
@API(API.Status.EXPERIMENTAL)
public abstract class Source {
    @Nonnull
    private final Set<Source> dependentSources;

    public Source() {
        this(new HashSet<>());
    }

    public Source(@Nonnull Set<Source> dependentSources) {
        this.dependentSources = new HashSet<>(dependentSources);
    }

    /**
     * Get the set of all sources of ancestor's of this source, include this source itself.
     * @return a set of the sources of all (exclusive) ancestors of this source
     */
    public abstract Set<Source> getSources();

    public void addDependentSource(@Nonnull Source source) {
        dependentSources.add(source);
    }

    /**
     * Check whether this source can be replaced by the given source in the given {@code ViewExpressionComparisons}.
     * In general, this source can be replaced by the given source if and only if:
     * <ul>
     *     <li>The sources match each other "structurally" (i.e., differ only in a pointer-equality sense).</li>
     *     <li>The elements with comparisons in the {@code ViewExpressionComparisons} do not use this source.</li>
     * </ul>
     * @param comparisons a view expression comparisons that specifies which sources are used and unused
     * @param other a candidate replacement for this source
     * @return whether this source can be replaced by {@code other}
     * @see #withSourceMappedInto(Source, Source)
     * @see Element#matchSourcesWith(ViewExpressionComparisons, Element) 
     */
    public abstract boolean supportsSourceIn(@Nonnull ViewExpressionComparisons comparisons, @Nonnull Source other);

    /**
     * Replace the given duplicate source with the given original source everywhere that it occurs in this source,
     * including in ancestor sources.
     * @param originalSource a source to replace all occurrences of the duplicate source with
     * @param duplicateSource a source to replace with the original source
     * @return a copy of this source with all occurrences of the duplicate source replaced with the original source
     */
    @Nonnull
    public abstract Source withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource);

    /**
     * Produce the stream of source entries that this source represents, drawing the values for dependent sources
     * from the given source entry if needed.
     * @param entry a source entry to draw the input for this source from
     * @return a stream of source entries, each of which has a value associated with this source
     */
    @Nonnull
    protected abstract Stream<SourceEntry> evalSourceEntriesFor(@Nonnull SourceEntry entry);

    /**
     * Produce the stream of source entries that this source represents, drawing the value for the input of this source
     * from the given {@link MessageOrBuilder}.
     * @param message a message to use when evaluating this source
     * @return a stream of source entries, each of which has a value associated with this source
     */
    public final Stream<SourceEntry> evalSourceEntriesFor(@Nonnull MessageOrBuilder message) {
        SourceEntry initial = SourceEntry.from(this, message);
        Stream<SourceEntry> entries = evalSourceEntriesFor(initial);
        for (Source dependentSource : dependentSources) {
            // TODO make lazier?
            entries = entries.flatMap(dependentSource::evalSourceEntriesFor);
        }
        return entries;
    }
}
