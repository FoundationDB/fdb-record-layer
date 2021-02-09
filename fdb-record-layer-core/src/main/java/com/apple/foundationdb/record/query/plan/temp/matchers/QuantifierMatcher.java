/*
 * QuantifierMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;

import javax.annotation.Nonnull;

/**
 * Matches a subclass of {@link com.apple.foundationdb.record.query.plan.temp.Quantifier} and a given matcher against the children.
 * @param <T> the type of {@link com.apple.foundationdb.record.query.plan.temp.Quantifier} to match against
 */
public class QuantifierMatcher<T extends Quantifier> extends TypeMatcher<T> {
    /**
     * Private constructor. Use static factory methods.
     * @param quantifierClass the class of the quantifier
     * @param childrenMatcher matcher for children
     */
    private QuantifierMatcher(@Nonnull final Class<? extends T> quantifierClass, @Nonnull final ExpressionChildrenMatcher childrenMatcher) {
        super(quantifierClass, childrenMatcher);
    }

    /**
     * Matches any {@link com.apple.foundationdb.record.query.plan.temp.Quantifier} quantifier together
     * with the given matcher for its {@code rangesOver()}.
     * @param rangesOverMatcher matcher for the rangesOver expression reference
     * @return a matcher matching a quantifier together with the given matcher for the reference it ranges over
     */
    @Nonnull
    public static QuantifierMatcher<Quantifier> any(@Nonnull ExpressionMatcher<? extends Bindable> rangesOverMatcher) {
        return ofKind(Quantifier.class, rangesOverMatcher);
    }

    /**
     * Matches any {@link com.apple.foundationdb.record.query.plan.temp.Quantifier} quantifier.
     * @return a matcher matching a quantifier\
     */
    @Nonnull
    public static QuantifierMatcher<Quantifier> any() {
        return new QuantifierMatcher<>(Quantifier.class, AnyChildrenMatcher.ANY);
    }


    /**
     * Matches a {@link com.apple.foundationdb.record.query.plan.temp.Quantifier.ForEach} quantifier together
     * with the given matcher for its {@code rangesOver()}.
     * @param rangesOverMatcher matcher for the rangesOver expression reference
     * @return a matcher matching a for each quantifier together with the given matcher for reference it ranges over.
     */
    @Nonnull
    public static QuantifierMatcher<Quantifier.ForEach> forEach(@Nonnull ExpressionMatcher<? extends Bindable> rangesOverMatcher) {
        return ofKind(Quantifier.ForEach.class, rangesOverMatcher);
    }

    /**
     * Matches a {@link com.apple.foundationdb.record.query.plan.temp.Quantifier.Existential} quantifier together
     * with the given matcher for its {@code rangesOver()}.
     * @param rangesOverMatcher matcher for the rangesOver expression reference
     * @return a matcher matching an existential quantifier together with the given matcher for the reference it ranges over
     */
    @Nonnull
    public static QuantifierMatcher<Quantifier.Existential> existential(@Nonnull ExpressionMatcher<? extends Bindable> rangesOverMatcher) {
        return ofKind(Quantifier.Existential.class, rangesOverMatcher);
    }

    /**
     * Matches a {@link com.apple.foundationdb.record.query.plan.temp.Quantifier.Physical} quantifier together
     * with the given matcher for its {@code rangesOver()}.
     * @param rangesOverMatcher matcher for the rangesOver expression reference
     * @return a matcher matching a physical quantifier together with the given matcher for the reference it ranges over
     */
    @Nonnull
    public static QuantifierMatcher<Quantifier.Physical> physical(@Nonnull ExpressionMatcher<? extends Bindable> rangesOverMatcher) {
        return ofKind(Quantifier.Physical.class, rangesOverMatcher);
    }

    /**
     * Matches a subclass of {@link com.apple.foundationdb.record.query.plan.temp.Quantifier} together
     * with the given matcher for its {@code rangesOver()}.
     * @param quantifierClass class specific flavor of quantifier to match
     * @param rangesOverMatcher matcher for the rangesOver expression reference
     * @param <Q> class of specific flavor of quantifier to match
     * @return a matcher matching a for each quantifier together with the given matcher for the reference it ranges over
     */
    @Nonnull
    public static <Q extends Quantifier> QuantifierMatcher<Q> ofKind(@Nonnull Class<? extends Q> quantifierClass,
                                                                     @Nonnull ExpressionMatcher<? extends Bindable> rangesOverMatcher) {
        return new QuantifierMatcher<>(quantifierClass, AnyChildMatcher.anyMatching(rangesOverMatcher));
    }
}
