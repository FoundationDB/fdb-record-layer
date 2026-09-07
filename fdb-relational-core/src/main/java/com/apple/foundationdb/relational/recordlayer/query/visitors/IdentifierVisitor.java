/*
 * IdentifierVisitor.java
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

package com.apple.foundationdb.relational.recordlayer.query.visitors;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.collect.ImmutableList;
import java.util.List;

@API(API.Status.EXPERIMENTAL)
public final class IdentifierVisitor extends DelegatingVisitor<BaseVisitor> {

    private IdentifierVisitor(BaseVisitor baseVisitor) {
        super(baseVisitor);
    }

    public static IdentifierVisitor of(BaseVisitor baseVisitor) {
        return new IdentifierVisitor(baseVisitor);
    }

    @Override
    public Identifier visitTableName(RelationalParser.TableNameContext tableNameContext) {
        return visitFullId(tableNameContext.fullId());
    }

    @Override
    public Identifier visitFullId(RelationalParser.FullIdContext fullIdContext) {
        Assert.thatUnchecked(!fullIdContext.uid().isEmpty());
        final ImmutableList.Builder<String> qualifierBuilder = ImmutableList.builder();
        for (int i = 0; i < fullIdContext.uid().size() - 1; i++) {
            final var qualifierPart = visitUid(fullIdContext.uid().get(i)).getName();
            qualifierBuilder.add(qualifierPart);
        }
        final var name = visitUid(fullIdContext.uid().get(fullIdContext.uid().size() - 1)).getName();
        return Identifier.of(name, qualifierBuilder.build());
    }

    @Override
    public List<Identifier> visitFullIdList(RelationalParser.FullIdListContext fullIdListContext) {
        return fullIdListContext.fullId().stream().map(this::visitFullId).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Identifier visitUid(RelationalParser.UidContext uidContext) {
        if (uidContext.simpleId() != null) {
            return visitSimpleId(uidContext.simpleId());
        } else {
            return Identifier.of(getDelegate().normalizeString(uidContext.getText()));
        }
    }

    @Override
    public List<Identifier> visitUidList(RelationalParser.UidListContext uidListContext) {
        return uidListContext.uid().stream()
                .map(this::visitUid)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Identifier visitSimpleId(RelationalParser.SimpleIdContext simpleIdContext) {
        return Identifier.of(getDelegate().normalizeString(simpleIdContext.getText()));
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Identifier visitIndexColumnName(RelationalParser.IndexColumnNameContext ctx) {
        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "setting index column is not supported");
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Identifier visitCharsetName(RelationalParser.CharsetNameContext ctx) {
        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "setting charset is not supported");
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Identifier visitCollationName(RelationalParser.CollationNameContext ctx) {
        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "setting collation is not supported");
    }

    @Override
    public Identifier visitTableFunctionName(final RelationalParser.TableFunctionNameContext ctx) {
        return visitFullId(ctx.fullId());
    }
}
