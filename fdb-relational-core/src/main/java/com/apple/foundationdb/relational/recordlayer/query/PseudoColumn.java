/*
 * PseudoColumn.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedRecordValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Contains a set of utility methods that are relevant for parsing the AST.
 */
public enum PseudoColumn {
    ROW_VERSION(qun -> new VersionValue(QuantifiedRecordValue.of(qun))),
    ;

    private static final String PREFIX = "__";

    @Nonnull
    private final Function<Quantifier, Value> valueCreator;
    @Nonnull
    private final String columnName;

    PseudoColumn(@Nonnull Function<Quantifier, Value> valueCreator) {
        this.valueCreator = valueCreator;
        this.columnName = PREFIX + name();
    }

    @Nonnull
    public Value getValue(@Nonnull Quantifier qun) {
        return Objects.requireNonNull(valueCreator.apply(qun));
    }

    @Nonnull
    public String getColumnName() {
        return columnName;
    }

    @Nonnull
    public static Optional<Expression> mapToExpressionMaybe(@Nonnull LogicalOperator logicalOperator,
                                                            @Nonnull String pseudoColumnName) {
        if (!pseudoColumnName.startsWith(PREFIX)) {
            return Optional.empty();
        }
        for (PseudoColumn pseudo : PseudoColumn.values()) {
            if (pseudo.columnName.equals(pseudoColumnName)) {
                return Optional.of(Expression.of(pseudo.getValue(logicalOperator.getQuantifier()),
                        Identifier.of(pseudoColumnName)));
            }
        }
        return Optional.empty();
    }
}
