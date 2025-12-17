/*
 * CopyPlan.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DataInKeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePathSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceProto;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Query plan for COPY command operations (export and import).
 */
@API(API.Status.EXPERIMENTAL)
public class CopyPlan extends QueryPlan {

    /**
     * Creates a COPY export plan.
     *
     * @param path the KeySpace path to export from (e.g., "/FRL/MY_DATABASE")
     *
     * @return a CopyPlan for exporting data
     */
    @Nonnull
    public static CopyPlan getCopyExportAction(@Nonnull String path,
                                               @Nonnull QueryExecutionContext queryExecutionContext) {
        // Export returns a single BYTES column (not nullable)
        Type rowType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(
                        Type.primitiveType(Type.TypeCode.BYTES, false),
                        java.util.Optional.of("DATA"),
                        java.util.Optional.of(0))));
        return new CopyPlan(CopyType.EXPORT, path, rowType, queryExecutionContext);
    }

    /**
     * Creates a COPY import plan.
     *
     * @param path the KeySpace path to import into (e.g., "/FRL/MY_DATABASE")
     *
     * @return a CopyPlan for importing data
     */
    @Nonnull
    public static CopyPlan getCopyImportAction(@Nonnull String path,
                                               @Nonnull QueryExecutionContext queryExecutionContext) {
        // Import returns a single INT column (not nullable) for count
        Type rowType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(
                        Type.primitiveType(Type.TypeCode.INT, false),
                        java.util.Optional.of("COUNT"),
                        java.util.Optional.of(0))));

        return new CopyPlan(CopyType.IMPORT, path, rowType, queryExecutionContext);
    }

    private enum CopyType {
        EXPORT,
        IMPORT
    }

    @Nonnull
    private final CopyType copyType;

    @Nonnull
    private final String path;

    @Nonnull
    private final Type rowType;

    @Nonnull
    private final QueryExecutionContext queryExecutionContext;

    CopyPlan(@Nonnull CopyType copyType,
             @Nonnull String path,
             @Nonnull Type rowType,
             @Nonnull QueryExecutionContext queryExecutionContext) {
        super("COPY " + copyType.name() + " " + path);
        this.copyType = copyType;
        this.path = path;
        this.rowType = rowType;
        this.queryExecutionContext = queryExecutionContext;
    }

    @Override
    public boolean isUpdatePlan() {
        return copyType == CopyType.IMPORT;
    }

    @Override
    public Plan<RelationalResultSet> optimize(@Nonnull CascadesPlanner planner,
                                              @Nonnull PlanContext planContext,
                                              @Nonnull PlanHashable.PlanHashMode currentPlanHashMode) {
        // No optimization needed for COPY operations
        return this;
    }

    @Override
    protected RelationalResultSet executeInternal(@Nonnull ExecutionContext context) throws RelationalException {
        switch (copyType) {
            case EXPORT:
                return executeExport(context);
            case IMPORT:
                return executeImport(context);
            default:
                throw new RelationalException("Unknown COPY type: " + copyType, ErrorCode.INTERNAL_ERROR);
        }
    }


    @SuppressWarnings("PMD.CloseResource") // Connection/cursor not owned by this method
    private RelationalResultSet executeExport(@Nonnull ExecutionContext context) throws RelationalException {
        try {
            final KeySpacePath keySpacePath = getPath();

            // Unwrap Transaction to FDBRecordContext
            final FDBRecordContext fdbContext = getRecordContext(context);

            // Create KeySpacePathSerializer for serialization
            KeySpacePathSerializer serializer = new KeySpacePathSerializer(keySpacePath);

            ScanProperties scanProperties = ScanProperties.FORWARD_SCAN;
            final Integer limit = context.getOptions().getOption(Options.Name.MAX_ROWS);
            if (limit > 0 && limit < Integer.MAX_VALUE) {
                scanProperties = scanProperties.with(executeProperties -> executeProperties.setReturnedRowLimit(limit));
            }
            // Export all data from the path (up to the requested limit)
            RecordCursor<DataInKeySpacePath> cursor =
                    keySpacePath.exportAllData(fdbContext, null, scanProperties);

            // Transform DataInKeySpacePath to Row with serialized bytes
            var iterator = RecordLayerIterator.create(cursor, data -> {
                if (data == null) {
                    return null;
                }
                try {
                    byte[] bytes = serializer.serialize(data).toByteArray();
                    return new ArrayRow(new Object[] { bytes });
                } catch (Exception e) {
                    throw new UncheckedRelationalException(
                            new RelationalException("Failed to serialize data",
                                    ErrorCode.COPY_SERIALIZATION_ERROR, e));
                }
            });

            // Build metadata for single BYTES column
            DataType.StructType structType = DataType.StructType.from("COPY_EXPORT", List.of(
                    DataType.StructType.Field.from("DATA", DataType.Primitives.BYTES.type(), 0)), true);
            RelationalStructMetaData structMetaData = RelationalStructMetaData.of(structType);

            // Return RecordLayerResultSet with continuation support
            // Row limiting via Statement.setMaxRows() is handled at higher level
            return new RecordLayerResultSet(structMetaData, iterator,
                    null /* caller is responsible for managing tx state */);
        } catch (RelationalException e) {
            throw e;
        } catch (Exception e) {
            throw new RelationalException("Failed to execute COPY export",
                    ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @SuppressWarnings("PMD.CloseResource") // Connection not owned by this method
    private RelationalResultSet executeImport(@Nonnull ExecutionContext context) throws RelationalException {
        try {
            // Ensure we have query execution context
            if (queryExecutionContext == null) {
                throw new RelationalException(
                        "COPY import requires query execution context for parameter binding",
                        ErrorCode.INTERNAL_ERROR);
            }

            // Get KeySpace from RelationalKeyspaceProvider singleton
            final KeySpacePath keySpacePath = getPath();

            final FDBRecordContext fdbContext = getRecordContext(context);
            final List<Object> dataArray = getDataForImport();
            KeySpacePathSerializer serializer = new KeySpacePathSerializer(keySpacePath);

            // Import each element
            int importCount = 0;
            for (Object element : dataArray) {
                final byte[] rawBytes = convertToBytes(element);
                final DataInKeySpacePath dataInKeySpacePath = deserializeData(serializer, rawBytes);
                importCount = importData(keySpacePath, fdbContext, dataInKeySpacePath, importCount);
            }

            // Build result with count
            DataType.StructType structType = DataType.StructType.from("COPY_IMPORT", List.of(), false);
            RelationalStructMetaData structMetaData = RelationalStructMetaData.of(structType);

            // Return result set with single row containing count
            // TODO this feels wrong, should it only import as you advance the result set?
            //      I don't think it matters if coming straight from AbstractEmbeddedStatement since that always just
            //      consumes the whole thing, but still seems like its not fitting in the abstraction
            final Iterator<ArrayRow> results = Iterators.transform(dataArray.iterator(), data -> new ArrayRow());
            return new IteratorResultSet(structMetaData, results, importCount);
        } catch (RelationalException | UncheckedRelationalException e) {
            throw e;
        } catch (Exception e) {
            throw new RelationalException("Failed to execute COPY import",
                    ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Nonnull
    private List<Object> getDataForImport() throws RelationalException {
        final List<OrderedLiteral> orderedLiterals = queryExecutionContext.getLiterals().getOrderedLiterals();
        if (orderedLiterals.isEmpty()) {
            throw new RelationalException(
                    "Parameter is not found",
                    ErrorCode.INVALID_PARAMETER);
        }
        Object parameterValue = orderedLiterals.get(0).getLiteralObject();

        if (parameterValue == null) {
            throw new RelationalException(
                    "Parameter is null or not found",
                    ErrorCode.INVALID_PARAMETER);
        }

        // Validate it's an array
        if (!(parameterValue instanceof List)) {
            throw new RelationalException(
                    "Parameter must be an ARRAY, got: " + parameterValue.getClass().getName(),
                    ErrorCode.INVALID_PARAMETER);
        }

        @SuppressWarnings("unchecked")
        List<Object> dataArray = (List<Object>) parameterValue;
        return dataArray;
    }

    @Nonnull
    private static byte[] convertToBytes(final Object element) throws RelationalException {
        if (!(element instanceof byte[])) {
            throw new RelationalException(
                    "Array elements must be BYTES, got: " + element.getClass().getName(),
                    ErrorCode.INVALID_PARAMETER);
        }

        return (byte[])element;
    }

    @Nonnull
    private static DataInKeySpacePath deserializeData(final KeySpacePathSerializer serializer, final byte[] byteString) throws RelationalException {
        DataInKeySpacePath dataInKeySpacePath;
        try {
            dataInKeySpacePath = serializer.deserialize(KeySpaceProto.DataInKeySpacePath.parseFrom(byteString));
        } catch (Exception e) {
            throw new RelationalException("Failed to deserialize data",
                    ErrorCode.COPY_SERIALIZATION_ERROR, e);
        }
        return dataInKeySpacePath;
    }

    private static int importData(final KeySpacePath keySpacePath, final FDBRecordContext fdbContext, final DataInKeySpacePath dataInKeySpacePath, int importCount) throws RelationalException {
        try {
            keySpacePath.importData(fdbContext, Collections.singleton(dataInKeySpacePath)).join();
            importCount++;
        } catch (Exception e) {
            throw new RelationalException("Failed to import data",
                    ErrorCode.COPY_IMPORT_VALIDATION_ERROR, e);
        }
        return importCount;
    }

    @Nonnull
    @Override
    public QueryPlanConstraint getConstraint() {
        return QueryPlanConstraint.noConstraint();
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals") // we don't want to create a new instance if the queryExecutionContext is the *same*
    @Nonnull
    @Override
    public Plan<RelationalResultSet> withExecutionContext(@Nonnull QueryExecutionContext queryExecutionContext) {
        if (queryExecutionContext == this.queryExecutionContext) {
            return this;
        }
        return new CopyPlan(copyType, path, rowType, queryExecutionContext);
    }

    @Nonnull
    @Override
    public String explain() {
        return "CopyPlan(" + copyType + ", path=" + path + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return rowType;
    }

    @Nonnull
    private KeySpacePath getPath() throws RelationalException {
        // Get KeySpace from RelationalKeyspaceProvider singleton
        KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();

        // Convert path string to KeySpacePath
        KeySpacePath keySpacePath;
        try {
            keySpacePath = KeySpaceUtils.toKeySpacePath(URI.create(path), keySpace);
        } catch (Exception e) {
            throw new RelationalException("Invalid COPY path: " + path,
                    ErrorCode.INVALID_COPY_PATH, e);
        }
        return keySpacePath;
    }

    private static FDBRecordContext getRecordContext(final @Nonnull ExecutionContext context) throws InternalErrorException {
        return context.transaction.unwrap(RecordContextTransaction.class).getContext();
    }
}
