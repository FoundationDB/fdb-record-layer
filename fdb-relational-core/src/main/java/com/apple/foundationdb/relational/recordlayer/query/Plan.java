/*
 * Plan.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.base.VerifyException;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public interface Plan<T> {

    class ExecutionContext {
        @Nonnull
        final Transaction transaction;
        @Nonnull
        final Options options;
        @Nonnull
        final RelationalConnection connection;

        ExecutionContext(@Nonnull Transaction transaction,
                         @Nonnull Options options,
                         @Nonnull RelationalConnection connection) {
            this.transaction = transaction;
            this.options = options;
            this.connection = connection;
        }

        @Nonnull
        public static ExecutionContext of(@Nonnull Transaction transaction,
                                              @Nonnull Options options,
                                              @Nonnull RelationalConnection connection) {
            return new ExecutionContext(transaction, options, connection);
        }
    }

    T execute(@Nonnull final ExecutionContext c) throws RelationalException;

    /**
     * Parses a query and generates an equivalent logical plan.
     *
     * @param query       The query string, required for logging.
     * @param planContext The plan context.
     * @return The logical plan of the query.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    static Plan<?> generate(@Nonnull final String query, @Nonnull PlanContext planContext) throws RelationalException {
        final RelationalParser.RootContext ast = AstVisitor.parseQuery(query);
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        registerMessageTypes(builder, planContext.getMetaData().getUnionDescriptor());
        final RelationalParserContext astContext = new RelationalParserContext(new Scopes(), builder, planContext.getMetaData().getRecordTypes().keySet(),
                planContext.getMetaData().getFieldDescriptorMapFromNames(planContext.getMetaData().getRecordTypes().keySet()),
                planContext.getMetaData().getAllIndexes().stream().map(Index::getName).collect(Collectors.toSet()));
        final AstVisitor astWalker = new AstVisitor(astContext, query, planContext.getConstantActionFactory(), planContext.getDdlQueryFactory(), planContext.getDbUri());
        try {
            final Object maybePlan = astWalker.visit(ast);
            Assert.that(maybePlan instanceof Plan, String.format("Could not generate a logical plan for query '%s'", query));
            return (Plan<?>) maybePlan;
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        } catch (MetaDataException mde) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde);
        } catch (VerifyException | SemanticException ve) {
            throw new RelationalException(ve.getMessage(), ErrorCode.INTERNAL_ERROR, ve);
        }
    }

    private static void registerMessageTypes(@Nonnull final TypeRepository.Builder builder, @Nonnull final Descriptors.Descriptor descriptor) {
        for (var field : descriptor.getFields()) {
            switch (field.getType()) {
                case MESSAGE: {
                    var typeDesc = field.getMessageType();
                    var oldType = builder.getTypeByName(typeDesc.getName());
                    var newType = ReferentialRecord.fromNamelessRecordType(typeDesc.getName(), Type.Record.fromDescriptor(typeDesc));
                    if (oldType.isEmpty()) {
                        builder.registerTypeToTypeNameMapping(newType, typeDesc.getName());
                        registerMessageTypes(builder, typeDesc);
                    } else {
                        Assert.thatUnchecked(oldType.get().equals(newType), String.format("Cannot register different types ('%s', '%s') with the same name", oldType, newType), ErrorCode.INVALID_NAME);
                    }
                    break;
                }
                case ENUM: {
                    var typeDesc = field.getEnumType();
                    var oldType = builder.getTypeByName(typeDesc.getName());
                    var newType = new Type.Enum(true, Type.Enum.enumValuesFromProto(typeDesc.getValues()));
                    if (oldType.isEmpty()) {
                        builder.registerTypeToTypeNameMapping(newType, typeDesc.getName()).addEnumType(typeDesc.toProto());
                    } else {
                        Assert.thatUnchecked(oldType.get().equals(newType), String.format("Cannot register different enums ('%s', '%s') with the same name", oldType, newType), ErrorCode.INVALID_NAME);
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }
}
