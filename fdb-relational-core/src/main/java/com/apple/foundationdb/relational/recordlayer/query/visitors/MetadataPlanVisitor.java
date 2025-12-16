/*
 * MetadataPlanVisitor.java
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
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.query.CopyPlanFactory;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;

import javax.annotation.Nonnull;
import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public final class MetadataPlanVisitor extends DelegatingVisitor<BaseVisitor> {

    private MetadataPlanVisitor(@Nonnull BaseVisitor baseVisitor) {
        super(baseVisitor);
    }

    @Nonnull
    public static MetadataPlanVisitor of(@Nonnull BaseVisitor baseVisitor) {
        return new MetadataPlanVisitor(baseVisitor);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        if (ctx.path() != null) {
            final var databaseName = visitUid(ctx.path().uid());
            SemanticAnalyzer.validateDatabaseUri(databaseName);
            return QueryPlan.MetadataQueryPlan.of(ddlFactory.getListDatabasesQueryAction(URI.create(databaseName.getName())));
        }
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getListDatabasesQueryAction(getDelegate().getDbUri()));
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getListSchemaTemplatesQueryAction());
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        final var schemaId = visitUid(ctx.schemaId().path().uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        final var database = dbAndSchema.getLeft().orElse(getDelegate().getDbUri());
        final var schema = dbAndSchema.getRight();
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getDescribeSchemaQueryAction(database, schema));
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        final var schemaTemplateId = visitUid(ctx.uid());
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getDescribeSchemaTemplateQueryAction(schemaTemplateId.getName()));
    }

    @Nonnull
    @Override
    public QueryPlan visitCopyExportStatement(@Nonnull RelationalParser.CopyExportStatementContext ctx) {
        final var pathId = visitUid(ctx.path().uid());
        final PreparedParams preparedParams = PreparedParams.copyOf(getDelegate().getPlanGenerationContext().getPreparedParams());
        return CopyPlanFactory.getCopyExportAction(pathId.getName(), getDelegate().getPlanGenerationContext());
    }

    @Nonnull
    @Override
    public QueryPlan visitCopyImportStatement(@Nonnull RelationalParser.CopyImportStatementContext ctx) {
        final var pathId = visitUid(ctx.path().uid());
        // We must visit the parameter to ensure that it ends up in the Literals in the query execution context
        visitPreparedStatementParameter(ctx.preparedStatementParameter());
        return CopyPlanFactory.getCopyImportAction(pathId.getName(), getDelegate().getPlanGenerationContext());
    }
}
