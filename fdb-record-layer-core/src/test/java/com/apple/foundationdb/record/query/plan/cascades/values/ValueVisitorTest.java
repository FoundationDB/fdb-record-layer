/*
 * ValueVisitorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Feasibility check for the generated {@code ValueVisitor}: dispatch has to reach a specific visitation method for every
 * kind of {@link Value}, including the ones nested inside another Value class.
 * <p>
 * This lives in the {@code values} package on purpose. Some of the values exercised here have package-private or
 * protected constructors, and the point is to build real instances rather than to assert on the generated source text.
 * </p>
 */
class ValueVisitorTest {

    /**
     * Names the specific visitation method it landed in, so a test can tell specific dispatch from the fallback. Only a
     * handful of methods are overridden -- the rest fall through to {@link #visitDefault}, which is what makes
     * {@code ValueVisitorWithDefaults} usable over a hierarchy this size.
     */
    private static final class NamingVisitor implements ValueVisitorWithDefaults<String> {
        @Nonnull
        @Override
        public String visitDefault(@Nonnull final Value element) {
            return "default";
        }

        @Nonnull
        @Override
        public String visitLiteralValue(@Nonnull final LiteralValue<?> element) {
            return "literal";
        }

        @Nonnull
        @Override
        public String visitSum(@Nonnull final NumericAggregationValue.Sum element) {
            return "sum";
        }

        @Nonnull
        @Override
        public String visitBitmapConstructAgg(@Nonnull final NumericAggregationValue.BitmapConstructAgg element) {
            return "bitmapConstructAgg";
        }

        @Nonnull
        @Override
        public String visitMinEverValue(@Nonnull final IndexOnlyAggregateValue.MinEverValue element) {
            return "minEver";
        }
    }

    @Nonnull
    private static final NamingVisitor visitor = new NamingVisitor();

    @Nonnull
    private static Value literalLong() {
        return LiteralValue.ofScalar(1L);
    }

    /**
     * A generic Value. Before the generator was taught about type parameters this produced
     * {@code visitLiteralValue(LiteralValue<T> element)}, capturing the visitor's own result variable, and a raw cast
     * in the dispatch map that failed the build under {@code -Werror}.
     */
    @Test
    void dispatchesToAGenericTopLevelValue() {
        assertThat(visitor.visit(LiteralValue.ofScalar(42L))).isEqualTo("literal");
    }

    /**
     * The cases this whole exercise was about. Each of these is declared inside another Value class, so before the
     * generator descended into nested types they had no visitation method at all and landed in {@code visitDefault}.
     */
    @Test
    void dispatchesToValuesNestedInsideAnotherValue() {
        assertThat(visitor.visit(new NumericAggregationValue.Sum(
                NumericAggregationValue.PhysicalOperator.SUM_L, literalLong())))
                .isEqualTo("sum");

        assertThat(visitor.visit(new NumericAggregationValue.BitmapConstructAgg(
                NumericAggregationValue.PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, literalLong())))
                .isEqualTo("bitmapConstructAgg");

        assertThat(visitor.visit(new IndexOnlyAggregateValue.MinEverValue(
                IndexOnlyAggregateValue.PhysicalOperator.MIN_EVER_LONG, literalLong())))
                .isEqualTo("minEver");
    }

    /**
     * A Value the visitor does not override still dispatches, just to the fallback. This is the property that keeps a
     * visitor over 60-odd methods maintainable: adding a new Value does not break existing implementors.
     */
    @Test
    void unhandledValueFallsThroughToDefault() {
        assertThat(visitor.visit(new NullValue(Type.primitiveType(Type.TypeCode.LONG)))).isEqualTo("default");
    }

    /**
     * The dispatch map is keyed on the concrete class, so a nested Value has to appear under its own class literal
     * rather than being folded into its enclosing one.
     */
    @Test
    void theDispatchMapNamesNestedClassesIndividually() {
        assertThat(ValueVisitor.jumpMap)
                .containsKeys(LiteralValue.class,
                        NumericAggregationValue.Sum.class,
                        NumericAggregationValue.Min.class,
                        NumericAggregationValue.Max.class,
                        NumericAggregationValue.Avg.class,
                        NumericAggregationValue.BitmapConstructAgg.class,
                        IndexOnlyAggregateValue.MinEverValue.class,
                        IndexOnlyAggregateValue.MaxEverValue.class,
                        RelOpValue.BinaryRelOpValue.class,
                        RelOpValue.UnaryRelOpValue.class,
                        AbstractArrayConstructorValue.LightArrayConstructorValue.class);
        // the enclosing classes are abstract and must not be dispatch targets themselves
        assertThat(ValueVisitor.jumpMap)
                .doesNotContainKeys(NumericAggregationValue.class, IndexOnlyAggregateValue.class, RelOpValue.class);
    }

    /**
     * Counts every value in the tree, proving {@link SimpleValueVisitor} supplies the recursion that the generated
     * dispatch-only visitor does not.
     */
    private static final class CountingVisitor implements SimpleValueVisitor<Integer> {
        @Nonnull
        @Override
        public Integer evaluateAtValue(@Nonnull final Value value, @Nonnull final List<Integer> childResults) {
            return 1 + childResults.stream().mapToInt(Integer::intValue).sum();
        }
    }

    /**
     * Renders the tree bottom-up, and overrides one specific visitation method to show that specific dispatch and the
     * default fold compose: the aggregate is named, everything else falls through to the generic rendering.
     */
    private static final class RenderingVisitor implements SimpleValueVisitor<String> {
        @Nonnull
        @Override
        public String evaluateAtValue(@Nonnull final Value value, @Nonnull final List<String> childResults) {
            return value.getClass().getSimpleName()
                    + (childResults.isEmpty() ? "" : "(" + String.join(", ", childResults) + ")");
        }

        @Nonnull
        @Override
        public String visitSum(@Nonnull final NumericAggregationValue.Sum element) {
            return "SUM[" + String.join(", ", visitChildren(element)) + "]";
        }
    }

    /**
     * Prunes literals, to show that {@link SimpleValueVisitor#shouldVisit} keeps a subtree out of the fold entirely.
     */
    private static final class LiteralPruningVisitor implements SimpleValueVisitor<Integer> {
        @Override
        public boolean shouldVisit(@Nonnull final Value value) {
            return !(value instanceof LiteralValue);
        }

        @Nonnull
        @Override
        public Integer evaluateAtValue(@Nonnull final Value value, @Nonnull final List<Integer> childResults) {
            return 1 + childResults.stream().mapToInt(Integer::intValue).sum();
        }
    }

    @Nonnull
    private static Value sumOverLiteral() {
        return new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_L, literalLong());
    }

    @Test
    void simpleValueVisitorFoldsTheWholeTree() {
        // the aggregate plus its single literal child
        assertThat(new CountingVisitor().visit(sumOverLiteral())).isEqualTo(2);
        // a bare leaf is just itself
        assertThat(new CountingVisitor().visit(literalLong())).isEqualTo(1);
    }

    @Test
    void simpleValueVisitorLetsASpecificMethodTakeOverWhileStillRecursing() {
        assertThat(new RenderingVisitor().visit(sumOverLiteral())).isEqualTo("SUM[LiteralValue]");
    }

    @Test
    void shouldVisitPrunesASubtreeOutOfTheFold() {
        // the literal child is pruned, so only the aggregate itself is counted
        assertThat(new LiteralPruningVisitor().visit(sumOverLiteral())).isEqualTo(1);
    }
}
