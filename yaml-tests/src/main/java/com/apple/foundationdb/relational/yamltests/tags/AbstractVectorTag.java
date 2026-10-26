/*
 * Vector16Field.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.tags;

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.google.common.base.Verify;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Collectors;

public abstract class AbstractVectorTag implements CustomTag {

    public static final class VectorMatcher implements Matchable {
        @Nonnull
        private final SequenceNode yamlNode;
        private final Class<? extends RealVector> vectorClass;

        private final int precision;

        public VectorMatcher(@Nonnull final Node node, int precision, Class<? extends RealVector> vectorClass) {
            this.yamlNode = Assert.castUnchecked(node, SequenceNode.class);
            this.precision = precision;
            this.vectorClass = vectorClass;
        }

        @Override
        public String toString() {
            return prettyPrintYamlNode(yamlNode);
        }

        @Nonnull
        @Override
        public Matchers.ResultSetMatchResult matches(@Nullable Object other, int rowNumber, @Nonnull String cellRef) {
            final var maybeNull = Matchable.shouldNotBeNull(other, rowNumber, cellRef);
            if (maybeNull.isPresent()) {
                return maybeNull.get();
            }
            RealVector thisVector = Matchers.constructVectorFromString(precision, Assert.castUnchecked(yamlNode, SequenceNode.class));
            Verify.verify(vectorClass.isInstance(other), "Expected vector of class '%s' but was '%s'", vectorClass.getName(), thisVector.getClass().getName());
            Verify.verify(vectorClass.isInstance(thisVector), "Expected vector of class '%s' but was '%s'", vectorClass.getName(), thisVector.getClass().getName());
            if (Verify.verifyNotNull(other).equals(thisVector)) {
                return Matchers.ResultSetMatchResult.success();
            }
            return Matchable.prettyPrintError("expected vector '" + prettyPrintYamlNode(yamlNode) + "' got '" + other + "' instead", rowNumber, cellRef);
        }

        @Nonnull
        private static String prettyPrintYamlNode(@Nonnull final SequenceNode sequenceNode) {
            return sequenceNode.getTag() + sequenceNode.getValue().stream()
                    .map(v -> Assert.castUnchecked(v, ScalarNode.class).getValue()).collect(Collectors.joining(",", "[", "]"));
        }
    }
}
