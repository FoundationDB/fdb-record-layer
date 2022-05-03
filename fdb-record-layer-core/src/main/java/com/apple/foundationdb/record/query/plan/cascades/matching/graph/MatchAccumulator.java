/*
 * MatchAccumulator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.graph;

/**
 * Class to used to accumulate the individual match results (the results of applying the {@link MatchFunction}) into
 * an object of type {@code R}. This class provides an interface for clients that in its concept is not entirely unlike
 * {@link java.util.stream.Collector}. Instances of this class, however, are meant to be mutable while collectors
 * are essentially stateless lambda factories.
 *
 * The life cycle of an accumulator (after its creation) spans multiple calls to {@link #accumulate} which cause the
 * accumulator instance to update it's internal and hidden state followed by one or more calls to {@link #finish}
 * which produce a final result of type {@code R}.
 *
 * @param <M> m
 * @param <R> r
 */
public interface MatchAccumulator<M, R> {
    /**
     * Accumulates the {@link Iterable} handed in into the internal state of the instance.
     * @param m and iterable of type {@code M}
     */
    void accumulate(Iterable<M> m);

    /**
     * Compute and return a result based on the internal state of the instance. After calling this method, clients
     * should not call {@link #accumulate} again. Clients should discard this object after this method has been called.
     * Multiple calls to this method, however, must return the same stable result.
     * @return the result
     */
    R finish();
}
