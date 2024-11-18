/*
 * RecordLayerUserDefinedFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.relational.api.metadata.FunctionDefinition;
import com.apple.foundationdb.relational.api.metadata.Visitor;
import com.apple.foundationdb.relational.generated.RelationalParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import java.util.List;

public class RecordLayerUserDefinedFunction implements FunctionDefinition {
    @Nonnull private final String functionName;
    @Nonnull private final String inputTypeName;
    @Nonnull private final List<RelationalParser.ColumnTypeContext> returnColumnTypes;
    @Nonnull private final String param;
    @Nonnull private final List<RelationalParser.ExpressionContext> functionCalls;

    public RecordLayerUserDefinedFunction(@Nonnull String functionName, @Nonnull String inputTypeName, @Nonnull List<RelationalParser.ColumnTypeContext> returnColumnTypes, @Nonnull String param, @Nonnull List<RelationalParser.ExpressionContext> functionCalls) {
        this.functionName = functionName;
        this.inputTypeName = inputTypeName;
        this.returnColumnTypes = returnColumnTypes;
        this.param = param;
        this.functionCalls = functionCalls;
    }

    @Nonnull
    @Override
    public String getName() {
        return functionName;
    }

    @Override
    public void accept(@Nonnull Visitor visitor) {
        visitor.visit(this);
    }

    @Nonnull
    public String getInputTypeName() {
        return inputTypeName;
    }
}
