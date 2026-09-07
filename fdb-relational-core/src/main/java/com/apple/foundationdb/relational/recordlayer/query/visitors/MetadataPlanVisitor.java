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
import com.apple.foundationdb.relational.recordlayer.query.CopyPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

@API(API.Status.EXPERIMENTAL)
public final class MetadataPlanVisitor extends DelegatingVisitor<BaseVisitor> {

    private MetadataPlanVisitor(BaseVisitor baseVisitor) {
        super(baseVisitor);
    }

    public static MetadataPlanVisitor of(BaseVisitor baseVisitor) {
        return new MetadataPlanVisitor(baseVisitor);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        if (ctx.path() != null) {
            final var databaseName = visitUid(ctx.path().uid());
            SemanticAnalyzer.validateDatabaseUri(databaseName);
            return QueryPlan.MetadataQueryPlan.of(ddlFactory.getListDatabasesQueryAction(URI.create(databaseName.getName())));
        }
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getListDatabasesQueryAction(getDelegate().getDbUri()));
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getListSchemaTemplatesQueryAction());
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        final var schemaId = visitUid(ctx.schemaId().path().uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        final Optional<URI> databaseUri = Objects.requireNonNull(dbAndSchema.getLeft());
        final var database = databaseUri.orElse(getDelegate().getDbUri());
        final var schema = dbAndSchema.getRight();
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getDescribeSchemaQueryAction(database, schema));
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        final var ddlFactory = getDelegate().getDdlQueryFactory();
        final var schemaTemplateId = visitUid(ctx.uid());
        return QueryPlan.MetadataQueryPlan.of(ddlFactory.getDescribeSchemaTemplateQueryAction(schemaTemplateId.getName()));
    }

    @Override
    public QueryPlan visitCopyExportStatement(RelationalParser.CopyExportStatementContext ctx) {
        final var pathId = visitUid(ctx.path().uid());
        final boolean incrementIncarnation = ctx.incarnationOption().INCREMENT() != null;
        return CopyPlan.getCopyExportAction(pathId.getName(), getDelegate().getPlanGenerationContext(), incrementIncarnation);
    }

    @Override
    public QueryPlan visitCopyImportStatement(RelationalParser.CopyImportStatementContext ctx) {
        final var pathId = visitUid(ctx.path().uid());
        // We must visit the parameter to ensure that it ends up in the Literals in the query execution context
        visitPreparedStatementParameter(ctx.preparedStatementParameter());
        return CopyPlan.getCopyImportAction(pathId.getName(), getDelegate().getPlanGenerationContext());
    }
}
