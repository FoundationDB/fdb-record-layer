/*
 * DdlVisitor.java
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
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexEngineKind;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionKeys;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorOptionKey;
import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerView;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperators;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryParser;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.ddl.MaterializedViewIndexGenerator;
import com.apple.foundationdb.relational.recordlayer.query.ddl.OnSourceIndexGenerator;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.recordlayer.query.functions.UserDefinedFunctionBuilder;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public final class DdlVisitor extends DelegatingVisitor<BaseVisitor> {
    // The vector engines an option may apply to. HNSW is the engine used when the VECTOR_ENGINE option is absent.
    private static final Set<VectorIndexEngineKind> ANY_ENGINE =
            ImmutableSet.of(VectorIndexEngineKind.HNSW, VectorIndexEngineKind.GUARDIANN);
    private static final Set<VectorIndexEngineKind> HNSW_ONLY = ImmutableSet.of(VectorIndexEngineKind.HNSW);
    private static final Set<VectorIndexEngineKind> GUARDIANN_ONLY = ImmutableSet.of(VectorIndexEngineKind.GUARDIANN);

    // The curated set of vector index options exposed through SQL DDL, keyed by their (lower-cased) SQL option name.
    // Adding or removing an option, or changing which engine it applies to, is a one-line change here and needs no
    // grammar change.
    private static final Map<String, VectorSqlOption<?>> SUPPORTED_VECTOR_OPTIONS =
            ImmutableMap.<String, VectorSqlOption<?>>builder()
                    // shared by both engines
                    .put("metric", new VectorSqlOption<>(VectorIndexOptionKeys.METRIC, ANY_ENGINE,
                            value -> parseMetric(value.hnswMetric())))
                    .put("use_rabitq", new VectorSqlOption<>(VectorIndexOptionKeys.USE_RABITQ, ANY_ENGINE,
                            DdlVisitor::parseOptionBoolean))
                    .put("rabitq_num_ex_bits", new VectorSqlOption<>(VectorIndexOptionKeys.RABITQ_NUM_EX_BITS,
                            ANY_ENGINE, DdlVisitor::parseOptionInt))
                    .put("maintain_stats_probability", new VectorSqlOption<>(VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY,
                            ANY_ENGINE, DdlVisitor::parseOptionDouble))
                    .put("sample_vector_stats_probability", new VectorSqlOption<>(VectorIndexOptionKeys.SAMPLE_VECTOR_STATS_PROBABILITY,
                            ANY_ENGINE, DdlVisitor::parseOptionDouble))
                    .put("stats_threshold", new VectorSqlOption<>(VectorIndexOptionKeys.STATS_THRESHOLD, ANY_ENGINE,
                            DdlVisitor::parseOptionInt))
                    // HNSW-only
                    .put("connectivity", new VectorSqlOption<>(VectorIndexOptionKeys.HNSW_M, HNSW_ONLY,
                            DdlVisitor::parseOptionInt))
                    .put("ef_construction", new VectorSqlOption<>(VectorIndexOptionKeys.HNSW_EF_CONSTRUCTION, HNSW_ONLY,
                            DdlVisitor::parseOptionInt))
                    .put("m_max", new VectorSqlOption<>(VectorIndexOptionKeys.HNSW_M_MAX, HNSW_ONLY,
                            DdlVisitor::parseOptionInt))
                    .put("m_max_0", new VectorSqlOption<>(VectorIndexOptionKeys.HNSW_M_MAX_0, HNSW_ONLY,
                            DdlVisitor::parseOptionInt))
                    // Guardiann-only (curated subset)
                    .put("primary_cluster_min", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MIN,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("primary_cluster_max", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MAX,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("underreplicated_primary_cluster_max", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("replicated_cluster_max_writes", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("replicated_cluster_target", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_TARGET,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("replication_priority_min", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_REPLICATION_PRIORITY_MIN,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionDouble))
                    .put("insert_max_candidate_clusters", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_INSERT_MAX_CANDIDATE_CLUSTERS,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("delete_max_candidate_clusters", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_DELETE_MAX_CANDIDATE_CLUSTERS,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("split_num_nearest_clusters", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_SPLIT_NUM_NEAREST_CLUSTERS,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("merge_num_nearest_clusters", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_MERGE_NUM_NEAREST_CLUSTERS,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("reassign_num_neighboring_clusters", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_REASSIGN_NUM_NEIGHBORING_CLUSTERS,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .put("collapse_min_duplicates", new VectorSqlOption<>(VectorIndexOptionKeys.GUARDIANN_COLLAPSE_MIN_DUPLICATES,
                            GUARDIANN_ONLY, DdlVisitor::parseOptionInt))
                    .build();

    @Nonnull
    private final RecordLayerSchemaTemplate.Builder metadataBuilder;

    /*
    Whether the schema contains any nullable arrays.
    If it is true, we'll wrap array field in all indexes. If it is false, we won't wrap any array field in any indexes.
    It is a temporary work-around. We should wrap nullable array field in indexes. It'll be removed after this work is done.
    */
    private boolean containsNullableArray;

    @Nonnull
    private final MetadataOperationsFactory metadataOperationsFactory;

    @Nonnull
    private final URI dbUri;

    private DdlVisitor(@Nonnull BaseVisitor delegate,
                      @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                      @Nonnull URI dbUri) {
        super(delegate);
        this.metadataBuilder = RecordLayerSchemaTemplate.newBuilder();
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.dbUri = dbUri;
    }

    @Nonnull
    public static DdlVisitor of(@Nonnull BaseVisitor delegate,
                                @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                                @Nonnull URI dbUri) {
        return new DdlVisitor(delegate, metadataOperationsFactory, dbUri);
    }

    @Nonnull
    @Override
    public DataType visitFunctionColumnType(@Nonnull final RelationalParser.FunctionColumnTypeContext ctx) {
        return lookupType(ctx.customType, ctx.primitiveType(), true, ctx.ARRAY() != null);
    }

    /**
     * Visits the column definition and creates a corresponding metadata object. The column is assumed by to {@code Nullable}
     * by default. I.e., if the user mentions no nullability constraint, the type of the column becomes nullable, which
     * has implications on the internal representation of repeated types, see {@link com.apple.foundationdb.relational.util.NullableArrayUtils}
     * for more details.
     * @param ctx the parse tree.
     * @return a {@link RecordLayerTable} object that captures all the properties of the column as defined by the user.
     */
    @Nonnull
    @Override
    public RecordLayerColumn visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        final var columnId = visitUid(ctx.colName);
        final var isRepeated = ctx.ARRAY() != null;
        final var isNullable = ctx.columnConstraint() != null ? (Boolean) ctx.columnConstraint().accept(this) : true;
        // TODO: We currently do not support NOT NULL for any type other than ARRAY. This is because there is no way to
        //       specify not "nullability" at the RecordMetaData level. For ARRAY, specifying that is actually possible
        //       by means of NullableArrayWrapper. In essence, we don't actually need a wrapper per se for non-array types,
        //       but a way to represent it in RecordMetadata.
        Assert.thatUnchecked(isRepeated || isNullable, ErrorCode.UNSUPPORTED_OPERATION, "NOT NULL is only allowed for ARRAY column type");
        containsNullableArray = containsNullableArray || (isRepeated && isNullable);
        final var columnType = lookupType(ctx.columnType().customType, ctx.columnType().primitiveType(), isNullable, isRepeated);
        return RecordLayerColumn.newBuilder().setName(columnId.getName()).setDataType(columnType).build();
    }

    @Nonnull
    @Override
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        final var tableId = visitUid(ctx.uid());
        final var columns = ctx.columnDefinition().stream().map(this::visitColumnDefinition).collect(ImmutableList.toImmutableList());
        final var tableBuilder = RecordLayerTable.newBuilder(metadataBuilder.isIntermingleTables())
                .setName(tableId.getName())
                .addColumns(columns);
        if (ctx.primaryKeyDefinition().fullIdList() != null) {
            visitFullIdList(ctx.primaryKeyDefinition().fullIdList())
                    .stream()
                    .map(Identifier::fullyQualifiedName)
                    .forEach(tableBuilder::addPrimaryKeyPart);
        }
        return tableBuilder.build();
    }

    @Nonnull
    @Override
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        final var structId = visitUid(ctx.uid());
        final var columns = ctx.columnDefinition().stream().map(this::visitColumnDefinition).collect(ImmutableList.toImmutableList());
        final var structBuilder = RecordLayerTable.newBuilder(metadataBuilder.isIntermingleTables())
                .setName(structId.getName())
                .addColumns(columns);
        return structBuilder.build();
    }

    @Nonnull
    @Override
    public RecordLayerIndex visitIndexAsSelectDefinition(@Nonnull RelationalParser.IndexAsSelectDefinitionContext indexDefinitionContext) {
        final var indexId = visitUid(indexDefinitionContext.indexName);

        final var ddlCatalog = metadataBuilder.build();
        // parse the index SQL query using the newly constructed metadata.
        getDelegate().replaceSchemaTemplate(ddlCatalog);
        final var viewPlan = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                Assert.castUnchecked(indexDefinitionContext.queryTerm().accept(this), LogicalOperator.class).getQuantifier().getRangesOver().get());

        final var useLegacyBasedExtremumEver = indexDefinitionContext.indexAttributes() != null && indexDefinitionContext.indexAttributes().indexAttribute().stream().anyMatch(attribute -> attribute.LEGACY_EXTREMUM_EVER() != null);
        final var isUnique = indexDefinitionContext.UNIQUE() != null;
        final var generator = MaterializedViewIndexGenerator.from(viewPlan, useLegacyBasedExtremumEver);
        Assert.thatUnchecked(viewPlan instanceof LogicalSortExpression, ErrorCode.INVALID_COLUMN_REFERENCE, "Cannot create index and order by an expression that is not present in the projection list");
        return generator.generate(metadataBuilder, indexId.getName(), isUnique, containsNullableArray, false).build();
    }

    @Nonnull
    @Override
    public RecordLayerIndex visitIndexOnSourceDefinition(@Nonnull final RelationalParser.IndexOnSourceDefinitionContext indexDefinitionContext) {
        final var ddlCatalog = metadataBuilder.build();
        getDelegate().replaceSchemaTemplate(ddlCatalog);
        getDelegate().pushPlanFragment();
        final var sourceIdentifier = visitFullId(indexDefinitionContext.source);
        var logicalOperator = generateSourceAccessForIndex(sourceIdentifier);
        getDelegate().getCurrentPlanFragment().setOperator(logicalOperator);

        final Identifier indexId = visitUid(indexDefinitionContext.indexName);
        final var isUnique = indexDefinitionContext.UNIQUE() != null;
        @Nullable final var indexOptions = indexDefinitionContext.indexOptions();
        final var useLegacyExtremum = indexOptions != null && indexOptions.indexOption().stream()
                .anyMatch(option -> option.LEGACY_EXTREMUM_EVER() != null);
        final var indexGeneratorBuilder = OnSourceIndexGenerator.newBuilder()
                .setIndexName(indexId)
                .setIndexSource(getDelegate().getCurrentPlanFragment())
                .setSemanticAnalyzer(getDelegate().getSemanticAnalyzer())
                .setUseLegacyExtremum(useLegacyExtremum)
                .setUseNullableArrays(containsNullableArray)
                .setMetadataBuilder(metadataBuilder)
                .setUnique(isUnique);

        indexDefinitionContext.indexColumnList().indexColumnSpec().forEach(colSpec ->
                indexGeneratorBuilder.addKeyColumn(OnSourceIndexGenerator.IndexedColumn
                        .parseColSpec(colSpec, getDelegate().getIdentifierVisitor())));

        if (indexDefinitionContext.includeClause() != null) {
            indexDefinitionContext.includeClause().uidList().uid().forEach(uid -> {
                indexGeneratorBuilder.addValueColumn(OnSourceIndexGenerator.IndexedColumn
                        .parseUid(uid, getDelegate().getIdentifierVisitor()));
            });
        }

        getDelegate().popPlanFragment();
        return indexGeneratorBuilder.build().generate().build();
    }

    @Nonnull
    @Override
    public RecordLayerIndex visitVectorIndexDefinition(final RelationalParser.VectorIndexDefinitionContext indexDefinitionContext) {
        final var ddlCatalog = metadataBuilder.build();
        getDelegate().replaceSchemaTemplate(ddlCatalog);
        getDelegate().pushPlanFragment();
        final var sourceIdentifier = visitFullId(indexDefinitionContext.source);
        var logicalOperator = generateSourceAccessForIndex(sourceIdentifier);
        getDelegate().getCurrentPlanFragment().setOperator(logicalOperator);

        final Identifier indexId = visitUid(indexDefinitionContext.indexName);
        final var engine = parseVectorEngine(indexDefinitionContext.engine);
        final var indexOptions = parseVectorOptions(engine, indexDefinitionContext.vectorIndexOptions());
        final var indexGeneratorBuilder = OnSourceIndexGenerator.newBuilder()
                .setIndexName(indexId)
                .setIndexSource(getDelegate().getCurrentPlanFragment())
                .setSemanticAnalyzer(getDelegate().getSemanticAnalyzer())
                .addAllIndexOptions(indexOptions)
                .setMetadataBuilder(metadataBuilder)
                .setGenerateKeyValueExpressionWithEmptyKey(true)
                .setUseNullableArrays(containsNullableArray);

        indexDefinitionContext.indexColumnList().indexColumnSpec().forEach(colSpec ->
                indexGeneratorBuilder.addValueColumn(OnSourceIndexGenerator.IndexedColumn
                        .parseColSpec(colSpec, getDelegate().getIdentifierVisitor())));

        // parse the number of dimensions.
        final var indexedColumns = indexGeneratorBuilder.getValueColumns();
        Assert.thatUnchecked(indexedColumns.size() == 1, ErrorCode.UNSUPPORTED_OPERATION,
                () -> "invalid number of indexed columns, only one column is supported, found " + indexedColumns.size() + " columns");
        final var indexedCol = Iterables.getOnlyElement(indexedColumns).getIdentifier();
        final var type = getDelegate().getSemanticAnalyzer().resolveIdentifier(indexedCol, getDelegate().getCurrentPlanFragment())
                .getDataType();
        Assert.thatUnchecked(type.getCode() == DataType.Code.VECTOR, ErrorCode.SYNTAX_ERROR,
                () -> "indexed column must be of vector type, found '" + type.getCode() + "' instead");
        final var numberOfDimensions = ((DataType.VectorType)type).getDimensions();
        VectorIndexOptionKeys.NUM_DIMENSIONS.put(indexGeneratorBuilder::addIndexOption, numberOfDimensions);

        Assert.isNullUnchecked(indexDefinitionContext.includeClause(), ErrorCode.UNSUPPORTED_OPERATION,
                "INCLUDE clause is not supported for vector indexes");

        if (indexDefinitionContext.indexPartitionClause() != null) {
            indexDefinitionContext.indexPartitionClause().indexColumnSpec().forEach(colSpec ->
                    indexGeneratorBuilder.addKeyColumn(OnSourceIndexGenerator.IndexedColumn
                            .parseColSpec(colSpec, getDelegate().getIdentifierVisitor())));
        }

        getDelegate().popPlanFragment();
        return indexGeneratorBuilder.build().generate().setIndexType(IndexTypes.VECTOR).build();
    }

    @Nonnull
    private LogicalOperator generateSourceAccessForIndex(@Nonnull final Identifier sourceIdentifier) {
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        var logicalOperator = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                LogicalOperator.generateAccess(sourceIdentifier, Optional.empty(), Optional.empty(), Set.of(),
                        semanticAnalyzer, getDelegate().getCurrentPlanFragment(),
                        getDelegate().getLogicalOperatorCatalog()));

        if (semanticAnalyzer.tableExists(sourceIdentifier)) {
            final var output = logicalOperator.getOutput().expanded().rewireQov(logicalOperator.getQuantifier().getFlowedObjectValue());
            logicalOperator = LogicalOperator.generateSimpleSelect(output, LogicalOperators.ofSingle(logicalOperator),
                    Optional.empty(), Optional.empty(), ImmutableSet.of(), true);
        }

        return logicalOperator;
    }

    /**
     * The SQL-facing binding of a single vector index option: the typed record-layer {@code key} it maps to, the vector
     * engine(s) it is valid for, and how to {@code coerce} the parsed SQL value into the key's value type. This wrapper
     * (rather than a bare {@link VectorOptionKey}) is what lets the DDL layer (a) reject an option used with the wrong
     * engine — engine applicability is DDL policy that the engine-agnostic key does not carry — and (b) turn the parsed
     * grammar value into the key's typed value (the key's own parser is for the stored wire form, and some values, such
     * as the metric, arrive as a typed sub-rule). Keeping {@code T} on the record also lets {@link #writeTo} call
     * {@code key.put(...)} without the wildcard-capture problem a bare {@code VectorOptionKey<?>} map would hit at the
     * call site. Curating {@link #SUPPORTED_VECTOR_OPTIONS} is how the SQL option surface is curated.
     *
     * @param <T> the option's value type
     */
    private record VectorSqlOption<T>(@Nonnull VectorOptionKey<T> key,
                                      @Nonnull Set<VectorIndexEngineKind> engines,
                                      @Nonnull Function<RelationalParser.VectorIndexOptionValueContext, T> coerce) {
        void writeTo(@Nonnull final BiConsumer<String, String> sink,
                     @Nonnull final RelationalParser.VectorIndexOptionValueContext value) {
            key.put(sink, coerce.apply(value));
        }
    }

    @Nonnull
    private static VectorIndexEngineKind parseVectorEngine(@Nonnull final RelationalParser.VectorEngineContext engineContext) {
        return engineContext.GUARDIANN() != null ? VectorIndexEngineKind.GUARDIANN : VectorIndexEngineKind.HNSW;
    }

    private static int parseOptionInt(@Nonnull final RelationalParser.VectorIndexOptionValueContext value) {
        return Integer.parseInt(value.getText());
    }

    private static double parseOptionDouble(@Nonnull final RelationalParser.VectorIndexOptionValueContext value) {
        return Double.parseDouble(value.getText());
    }

    private static boolean parseOptionBoolean(@Nonnull final RelationalParser.VectorIndexOptionValueContext value) {
        return Boolean.parseBoolean(value.getText());
    }

    @Nonnull
    private Map<String, String> parseVectorOptions(@Nonnull final VectorIndexEngineKind engine,
                                                   @Nullable final RelationalParser.VectorIndexOptionsContext indexOptionsContext) {
        final var indexOptionsBuilder = ImmutableMap.<String, String>builder();
        // Guardiann must be recorded explicitly; HNSW is the default when the engine option is absent, so leave it
        // implicit (keeps HNSW index metadata unchanged and readable by nodes that predate the engine option).
        if (engine == VectorIndexEngineKind.GUARDIANN) {
            VectorIndexOptionKeys.ENGINE.put(indexOptionsBuilder::put, engine);
        }
        if (indexOptionsContext == null) {
            return indexOptionsBuilder.build();
        }
        final Set<String> seen = new HashSet<>();
        for (final var option : indexOptionsContext.vectorIndexOption()) {
            final String name = option.optionName.getText().toLowerCase(Locale.ROOT);
            final var spec = SUPPORTED_VECTOR_OPTIONS.get(name);
            if (spec == null) {
                throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION,
                        "unsupported vector index option '" + name + "'");
            }
            Assert.thatUnchecked(spec.engines().contains(engine), ErrorCode.UNSUPPORTED_OPERATION,
                    () -> "vector index option '" + name + "' is not valid for the " + engine.name() + " vector engine");
            Assert.thatUnchecked(seen.add(name), ErrorCode.SYNTAX_ERROR,
                    () -> "duplicate vector index option '" + name + "'");
            try {
                spec.writeTo(indexOptionsBuilder::put, option.optionValue);
            } catch (final NumberFormatException e) {
                throw Assert.failUnchecked(ErrorCode.SYNTAX_ERROR,
                        "invalid value '" + option.optionValue.getText() + "' for vector index option '" + name + "'", e);
            }
        }
        return indexOptionsBuilder.build();
    }

    /**
     * Maps a parsed {@code METRIC = ...} clause to its {@link Metric}. The typed metric option key then serializes it
     * back to the canonical wire form (the enum constant name), so the mapping lives here rather than being spelled as
     * raw {@code Metric.X.name()} strings at the write site.
     *
     * @param metricContext the parsed metric clause
     * @return the corresponding metric
     */
    @Nonnull
    private static Metric parseMetric(@Nonnull final RelationalParser.HnswMetricContext metricContext) {
        if (metricContext.DOT_PRODUCT_METRIC() != null) {
            return Metric.DOT_PRODUCT_METRIC;
        } else if (metricContext.EUCLIDEAN_METRIC() != null) {
            return Metric.EUCLIDEAN_METRIC;
        } else if (metricContext.EUCLIDEAN_SQUARE_METRIC() != null) {
            return Metric.EUCLIDEAN_SQUARE_METRIC;
        } else if (metricContext.COSINE_METRIC() != null) {
            return Metric.COSINE_METRIC;
        }
        throw Assert.failUnchecked("metric " + metricContext.getText() + " is not currently supported");
    }

    @Nonnull
    @Override
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        final var enumId = visitUid(ctx.uid());

        // (yhatem) we have control over the ENUM values' numbers.
        final List<DataType.EnumType.EnumValue> enumValues = new ArrayList<>(ctx.STRING_LITERAL().size());
        for (int i = 0; i < ctx.STRING_LITERAL().size(); i++) {
            enumValues.add(DataType.EnumType.EnumValue.of(Assert.notNullUnchecked(getDelegate().normalizeString(ctx.STRING_LITERAL(i).getText())), i));
        }
        return DataType.EnumType.from(enumId.getName(), enumValues, false);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        final var schemaTemplateId = visitUid(ctx.schemaTemplateId().uid());
        // schema template version will be set automatically at update operation to lastVersion + 1
        metadataBuilder.setName(schemaTemplateId.getName()).setVersion(1);
        if (ctx.optionsClause() != null) {
            for (var option : ctx.optionsClause().option()) {
                if (option.ENABLE_LONG_ROWS() != null) {
                    metadataBuilder.setEnableLongRows(option.booleanLiteral().TRUE() != null);
                } else if (option.INTERMINGLE_TABLES() != null) {
                    metadataBuilder.setIntermingleTables(option.booleanLiteral().TRUE() != null);
                } else if (option.STORE_ROW_VERSIONS() != null) {
                    metadataBuilder.setStoreRowVersions(option.booleanLiteral().TRUE() != null);
                } else {
                    Assert.failUnchecked(ErrorCode.SYNTAX_ERROR, "Encountered unknown options in schema template creation");
                }
            }
        }

        final ImmutableSet.Builder<RelationalParser.StructDefinitionContext> structClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.TableDefinitionContext> tableClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.IndexDefinitionContext> indexClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.SqlInvokedFunctionContext> sqlInvokedFunctionClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.ViewDefinitionContext> viewClauses = ImmutableSet.builder();
        for (final var templateClause : ctx.templateClause()) {
            if (templateClause.enumDefinition() != null) {
                metadataBuilder.addAuxiliaryType(visitEnumDefinition(templateClause.enumDefinition()));
            } else if (templateClause.structDefinition() != null) {
                structClauses.add(templateClause.structDefinition());
            } else if (templateClause.tableDefinition() != null) {
                tableClauses.add(templateClause.tableDefinition());
            } else if (templateClause.sqlInvokedFunction() != null) {
                sqlInvokedFunctionClauses.add(templateClause.sqlInvokedFunction());
            } else if (templateClause.viewDefinition() != null) {
                viewClauses.add(templateClause.viewDefinition());
            } else if (templateClause.storedQueryDefinition() != null) {
                final var queryCtx = templateClause.storedQueryDefinition();
                final var name = visitUid(queryCtx.queryName).getName();
                final var sourceText = getDelegate().getPlanGenerationContext().getQuery();
                // Parse the optional typed-named-parameter signature. Each parameter is referenced by name in the query
                // body and declared function bodies; those references are rewritten to '?name' (see
                // rewriteReferencesToParams). Matching is case-sensitive — a signature parameter becomes a named
                // prepared parameter, which is always case-sensitive, so a reference must use the declared spelling. A
                // parameter declared NULL is exactly-null, and is warmed by binding it to null just as setNull would,
                // while a parameter with a declared type is strictly of that type and non-null. Each parameter is
                // persisted as name -> type code so warm-up can type the value-free ones.
                final var parameters = new HashMap<String, String>();
                final var signatureCtx = queryCtx.storedQuerySignature();
                if (signatureCtx != null) {
                    for (final var param : signatureCtx.storedQueryParameter()) {
                        final var paramName = param.parameterName.getText();
                        // The reference rewrite only matches ID tokens, so a quoted or keyword-spelled name would be
                        // silently left alone in the body and resolved as a column instead of a parameter.
                        Assert.thatUnchecked(isSimpleIdentifier(param.parameterName), ErrorCode.UNSUPPORTED_QUERY,
                                () -> "stored query signature parameter '" + paramName + "' must be a simple identifier");
                        final Type.TypeCode typeCode;
                        if (param.NULL_LITERAL() != null) {
                            typeCode = Type.TypeCode.NULL;
                        } else {
                            // A declared type means strictly that type and non-null — the null case is declared as
                            // NULL instead — so only the type code is kept. The nullability that
                            // visitFunctionColumnType assigns is a general-purpose default and does not apply here.
                            final var paramType = DataTypeUtils.toRecordLayerType(visitFunctionColumnType(param.parameterType));
                            Assert.thatUnchecked(paramType.isPrimitive(), ErrorCode.UNSUPPORTED_QUERY,
                                    () -> "stored query signature parameter '" + paramName + "' must have a primitive or null type");
                            typeCode = paramType.getTypeCode();
                            // A boolean's value is plan-determining — predicates fold away, branches disappear, index
                            // choices differ — so it belongs in the body as a literal, where the planner specializes on
                            // it and the resulting IS_TRUE/IS_FALSE constraint keeps the two plans distinct. A
                            // value-free boolean could carry only "is not null", giving one plan for both values.
                            Assert.thatUnchecked(typeCode != Type.TypeCode.BOOLEAN, ErrorCode.UNSUPPORTED_QUERY,
                                    () -> "stored query signature parameter '" + paramName + "' may not be BOOLEAN; "
                                            + "write the boolean as a literal in the query body instead");
                        }
                        Assert.thatUnchecked(parameters.put(paramName, typeCode.name()) == null,
                                ErrorCode.UNSUPPORTED_QUERY,
                                () -> "duplicate stored query signature parameter '" + paramName + "'");
                    }
                }
                final var queryString = rewriteReferencesToParams(sourceText, queryCtx.storedQuery, parameters.keySet());
                final ImmutableList.Builder<String> tempFunctionTexts = ImmutableList.builder();
                if (queryCtx.declareBlock() != null) {
                    // Compared as identifiers rather than as raw text: a reference resolves case-insensitively unless
                    // the connection is case-sensitive, so a signature parameter `x` and a function parameter `X` are
                    // the same identifier and collide, even though the rewrite itself matches exactly.
                    final var normalizedParameterNames = parameters.keySet().stream()
                            .map(parameterName -> getDelegate().normalizeString(parameterName))
                            .collect(ImmutableSet.toImmutableSet());
                    for (final var dfCtx : queryCtx.declareBlock().declaredFunction()) {
                        // A signature parameter and one of this function's own parameters naming the same identifier are
                        // indistinguishable in the body, so the rewrite would capture the function's parameter instead
                        // of shadowing it. Reject rather than silently changing what was written.
                        final var shadowed = Sets.intersection(ownParameterNames(dfCtx), normalizedParameterNames);
                        Assert.thatUnchecked(shadowed.isEmpty(), ErrorCode.UNSUPPORTED_QUERY,
                                () -> "declared function parameter " + shadowed
                                        + " collides with a stored query signature parameter");
                        tempFunctionTexts.add(rewriteDeclaredFunctionToStandalone(dfCtx, sourceText, parameters.keySet()));
                    }
                }
                final var storedQueryTexts = tempFunctionTexts.build();
                // The rewritten text is what gets persisted and re-parsed at warm-up, so prove now that it parses.
                // This keeps a rewrite defect from becoming a startup-time failure that the author never sees.
                validateRewrittenText(name, queryString, storedQueryTexts);
                metadataBuilder.addStoredQuery(name, queryString, storedQueryTexts, parameters);
            } else {
                Assert.thatUnchecked(templateClause.indexDefinition() != null);
                indexClauses.add(templateClause.indexDefinition());
            }
        }
        structClauses.build().stream().map(this::visitStructDefinition).map(RecordLayerTable::getDatatype).forEach(metadataBuilder::addAuxiliaryType);
        tableClauses.build().stream().map(this::visitTableDefinition).forEach(metadataBuilder::addTable);
        // TODO: this is currently relying on the lexical order of the function to resolve function dependencies which
        //       is limited.
        sqlInvokedFunctionClauses.build().forEach(functionClause -> {
            metadataBuilder.addInvokedRoutine(getInvokedRoutineMetadata(functionClause, functionClause.functionSpecification(),
                    functionClause.routineBody(), metadataBuilder.build()));
        });
        viewClauses.build().forEach(viewClause -> {
            final var view = getViewMetadata(viewClause, metadataBuilder.build());
            metadataBuilder.addView(view);
        });
        final var indexes = indexClauses.build().stream().map(clause -> Assert.castUnchecked(visit(clause), RecordLayerIndex.class)).collect(ImmutableList.toImmutableList());
        for (final RecordLayerIndex index : indexes) {
            final var table = metadataBuilder.extractTable(index.getTableName());
            final var tableWithIndex = RecordLayerTable.Builder.from(table).addIndex(index).build();
            metadataBuilder.addTable(tableWithIndex);
        }
        return ProceduralPlan.of(metadataOperationsFactory.getSaveSchemaTemplateConstantAction(metadataBuilder.build(), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        final Identifier schemaId = visitUid(ctx.schemaId().path().uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        final var templateId = visitUid(ctx.schemaTemplateId().uid());
        return ProceduralPlan.of(metadataOperationsFactory.getCreateSchemaConstantAction(dbAndSchema.getLeft().orElse(dbUri),
                dbAndSchema.getRight(), templateId.getName(), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        final var databaseId = visitUid(ctx.path().uid());
        SemanticAnalyzer.validateDatabaseUri(databaseId);
        return ProceduralPlan.of(metadataOperationsFactory.getCreateDatabaseConstantAction(URI.create(databaseId.getName()), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        final var databaseId = visitUid(ctx.path().uid());
        SemanticAnalyzer.validateDatabaseUri(databaseId);
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return  ProceduralPlan.of(metadataOperationsFactory.getDropDatabaseConstantAction(URI.create(databaseId.getName()), throwIfDoesNotExist, Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        final var schemaId = visitUid(ctx.uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), ErrorCode.UNKNOWN_DATABASE, () -> String.format(Locale.ROOT, "invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(metadataOperationsFactory.getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        final var schemaTemplateId = visitUid(ctx.uid());
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getDropSchemaTemplateConstantAction(schemaTemplateId.getName(),
                throwIfDoesNotExist, Options.NONE));
    }

    @Nonnull
    private RecordLayerInvokedRoutine getInvokedRoutineMetadata(@Nonnull final ParserRuleContext functionCtx,
                                                                @Nonnull final RelationalParser.FunctionSpecificationContext functionSpecCtx,
                                                                @Nonnull final RelationalParser.RoutineBodyContext bodyCtx,
                                                                @Nonnull final RecordLayerSchemaTemplate ddlCatalog) {
        // parse the index SQL query using the newly constructed metadata.
        getDelegate().replaceSchemaTemplate(ddlCatalog);

        final var isTemporary = functionCtx instanceof RelationalParser.CreateTempFunctionContext;

        // 1. get the function name.
        final var functionName = visitFullId(functionSpecCtx.schemaQualifiedRoutineName).toString();

        // 2. get the function SQL definition string.
        final var queryString = getDelegate().getPlanGenerationContext().getQuery();
        final var start = functionCtx.start.getStartIndex();
        final var stop = functionCtx.stop.getStopIndex() + 1; // inclusive.
        final var functionDefinition = (isTemporary ? "" : "CREATE ") + queryString.substring(start, stop);
        if (isTemporary) {
            getDelegate().getPlanGenerationContext().getLiteralsBuilder().setScope(functionName);
        } else {
            // prepared non-temporary SQL functions are not supported.
            QueryParser.validateNoPreparedParams(functionCtx);
        }

        RecordLayerInvokedRoutine.Builder builder = RecordLayerInvokedRoutine.newBuilder()
                .setName(functionName)
                .setDescription(functionDefinition)
                .setTemporary(isTemporary)
                .setPreparedParams(PreparedParams.copyOf(getDelegate().getPlanGenerationContext().getPreparedParams()))
                .setNormalizedDescription(getDelegate().getPlanGenerationContext().getCanonicalQueryString());

        boolean isMacro = bodyCtx instanceof RelationalParser.UserDefinedMacroFunctionStatementBodyContext;
        if (!isMacro && isTemporary) {
            // delay the compilation of table-valued temporary functions for later
            return builder
                    .withUserDefinedFunctionProvider(ignore -> visitSqlInvokedFunction(functionSpecCtx, bodyCtx, isTemporary))
                    .withSerializableFunction(new RawSqlFunction(functionName, functionDefinition))
                    .build();
        } else {
            final var userDefinedFunction = visitSqlInvokedFunction(functionSpecCtx, bodyCtx, isTemporary);
            builder.withUserDefinedFunctionProvider(ignore -> userDefinedFunction);
            if (isMacro) {
                return builder.withSerializableFunction(userDefinedFunction).build();
            } else {
                return builder.withSerializableFunction(new RawSqlFunction(functionName, functionDefinition)).build();
            }
        }
    }

    @Nonnull
    private RecordLayerView getViewMetadata(@Nonnull final RelationalParser.ViewDefinitionContext viewCtx,
                                            @Nonnull final RecordLayerSchemaTemplate ddlCatalog) {
        // parse the view SQL query using the newly constructed metadata.
        getDelegate().replaceSchemaTemplate(ddlCatalog);

        // 1. get the view name.
        final var viewName = visitFullId(viewCtx.viewName).toString();

        // 2. get the view SQL definition string.
        final var queryString = getDelegate().getPlanGenerationContext().getQuery();
        final var start = viewCtx.viewQuery.start.getStartIndex();
        final var stop = viewCtx.viewQuery.stop.getStopIndex() + 1; // inclusive.
        final var viewDefinition = queryString.substring(start, stop);

        // prepared parameters in views are not supported.
        QueryParser.validateNoPreparedParams(viewCtx);

        getDelegate().pushPlanFragment();

        // 3. visit the SQL string to generate (compile) the corresponding SQL plan.
        final var viewQuery = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                Assert.castUnchecked(viewCtx.viewQuery.accept(this), LogicalOperator.class));

        getDelegate().popPlanFragment();

        // 4. Return it.
        return RecordLayerView.newBuilder()
                .setName(viewName)
                .setDescription(viewDefinition)
                .setViewCompiler(ignored -> viewQuery)
                .build();
    }

    @Override
    public ProceduralPlan visitCreateTempFunction(@Nonnull RelationalParser.CreateTempFunctionContext ctx) {
        final var invokedRoutine = getInvokedRoutineMetadata(ctx, ctx.tempSqlInvokedFunction().functionSpecification(),
                ctx.tempSqlInvokedFunction().routineBody(), getDelegate().getSchemaTemplate());
        var throwIfExists = ctx.REPLACE() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getCreateTemporaryFunctionConstantAction(getDelegate().getSchemaTemplate(),
                throwIfExists, invokedRoutine));
    }

    @Override
    public ProceduralPlan visitDropTempFunction(@Nonnull RelationalParser.DropTempFunctionContext ctx) {
        final var functionName = visitFullId(ctx.schemaQualifiedRoutineName).toString();
        var throwIfNotExists = ctx.IF() == null && ctx.EXISTS() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getDropTemporaryFunctionConstantAction(throwIfNotExists, functionName));
    }

    @Override
    public CompiledSqlFunction visitTempSqlInvokedFunction(@Nonnull RelationalParser.TempSqlInvokedFunctionContext ctx) {
        return (CompiledSqlFunction)visitSqlInvokedFunction(ctx.functionSpecification(), ctx.routineBody(), true);
    }

    @Override
    public UserDefinedFunction visitSqlInvokedFunction(@Nonnull RelationalParser.SqlInvokedFunctionContext ctx) {
        return visitSqlInvokedFunction(ctx.functionSpecification(), ctx.routineBody(), false);
    }

    private UserDefinedFunction visitSqlInvokedFunction(@Nonnull final RelationalParser.FunctionSpecificationContext functionSpecCtx,
                                                        @Nonnull final RelationalParser.RoutineBodyContext bodyCtx,
                                                        boolean isTemporary) {
        // run implementation-specific validations.
        final var props = functionSpecCtx.routineCharacteristics();

        // SQL-invoked routine 11.60, syntax rules, section 6.f.ii
        // ... currently we support only CALLED ON NULL INPUT (which is implicitly set if not defined).
        boolean isNullReturnOnNull = props.nullCallClause() != null && props.nullCallClause().RETURNS() != null;
        Assert.thatUnchecked(!isNullReturnOnNull, ErrorCode.UNSUPPORTED_OPERATION,
                "only CALLED ON NULL INPUT clause is supported");

        boolean isSqlParameterStyle = props.parameterStyle() == null || props.parameterStyle().SQL() != null;
        Assert.thatUnchecked(isSqlParameterStyle, ErrorCode.UNSUPPORTED_OPERATION,
                "only sql-style parameters are supported");

        // todo: rework Java UDFs to go through this code path as well.
        final var language = (props.languageClause() != null && props.languageClause().languageName().JAVA() != null)
                             ? InvokedRoutine.Language.JAVA
                             : InvokedRoutine.Language.SQL;
        Assert.thatUnchecked(language == InvokedRoutine.Language.SQL, ErrorCode.UNSUPPORTED_OPERATION,
                "only sql-language functions are supported");

        final var isMacro = bodyCtx instanceof RelationalParser.UserDefinedMacroFunctionStatementBodyContext;
        Assert.thatUnchecked(functionSpecCtx.returnsClause() == null || isMacro,
                ErrorCode.UNSUPPORTED_OPERATION,
                "unsupported explicit return type for SQL table function");

        final DataType returnsType = Optional.ofNullable(functionSpecCtx.returnsClause())
                .map(returnsClause ->
                        Assert.castUnchecked(visit(returnsClause), DataType.class))
                .orElse(null);
        Assert.thatUnchecked(returnsType == null || returnsType.isResolved(),
                ErrorCode.UNKNOWN_TYPE,
                "unknown explicit return type for function");

        final var functionName = visitFullId(functionSpecCtx.schemaQualifiedRoutineName).toString();
        final var parameters = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                visitSqlParameterDeclarationList(functionSpecCtx.sqlParameterDeclarationList()).asNamedArguments());
        final var sqlFunctionBodyStepBuilder = UserDefinedFunctionBuilder.newBuilder()
                .setName(functionName)
                .addAllParameters(parameters)
                .setReturnType(returnsType)
                .seal();

        // the nested fragment below serves two purposes:
        // 1. avoid creating a top-level LSE unnecessarily.
        // 2. add a fake quantifier with the function parameters (if any) to resolve their references in the function body
        //    during its plan generation.
        final var fragment = getDelegate().pushPlanFragment();
        final var parametersCorrelation = sqlFunctionBodyStepBuilder.getParametersCorrelation();
        parametersCorrelation.ifPresent(quantifier -> fragment.addOperator(LogicalOperator.newUnnamedOperator(
                Expressions.fromQuantifier(quantifier), quantifier)));

        final UserDefinedFunctionBuilder.FinalStepBuilder finalStepBuilder;
        if (isMacro) {
            // TODO: we should not disable literal processing for temporary macro functions, but UserDefinedMacroFunction
            //       doesn't support serializing its literals yet. This will be done as part of
            //       https://github.com/FoundationDB/fdb-record-layer/issues/4306.
            final var bodyValue = Assert.castUnchecked(
                    getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(
                            () -> visit(bodyCtx)), Expression.class).getUnderlying();
            finalStepBuilder = sqlFunctionBodyStepBuilder.withBodyValue(bodyValue);
        } else {
            Assert.thatUnchecked(bodyCtx instanceof RelationalParser.StatementBodyContext,
                    ErrorCode.INVALID_FUNCTION_DEFINITION,
                    () -> "Unsupported routine body specification for SQL invoked function");

            final LogicalOperator bodyOperator;
            if (isTemporary) {
                bodyOperator = Assert.castUnchecked(visit(bodyCtx), LogicalOperator.class);
            } else {
                bodyOperator = Assert.castUnchecked(
                        getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(
                                () -> visit(bodyCtx)), LogicalOperator.class);
            }
            finalStepBuilder = sqlFunctionBodyStepBuilder.withBodyExpression(bodyOperator.getQuantifier().getRangesOver().get())
                    .setLiterals(getDelegate().getPlanGenerationContext().getLiterals());
        }

        getDelegate().popPlanFragment();
        return finalStepBuilder.build();
    }

    @Nonnull
    @Override
    public Expression visitUserDefinedMacroFunctionStatementBody(@Nonnull final RelationalParser.UserDefinedMacroFunctionStatementBodyContext ctx) {
        return Assert.castUnchecked(visit(ctx.expression()), Expression.class);
    }

    @Override
    public LogicalOperator visitStatementBody(final RelationalParser.StatementBodyContext ctx) {
        return Assert.castUnchecked(visit(ctx.queryTerm()), LogicalOperator.class);
    }

    @Override
    public Expressions visitSqlParameterDeclarationList(@Nonnull RelationalParser.SqlParameterDeclarationListContext ctx) {
        if (ctx.sqlParameterDeclarations() == null) {
            return Expressions.empty();
        }
        return visitSqlParameterDeclarations(ctx.sqlParameterDeclarations());
    }

    @Override
    public Expressions visitSqlParameterDeclarations(final RelationalParser.SqlParameterDeclarationsContext ctx) {
        final var parameters = Expressions.of(ctx.sqlParameterDeclaration().stream().map(this::visitSqlParameterDeclaration).collect(ImmutableList.toImmutableList()));
        final var duplicateParameters = parameters.asList().stream().flatMap(p -> p.getName().stream())
                .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) )
                .entrySet()
                .stream()
                .filter( p -> p.getValue() > 1 )
                .collect(ImmutableList.toImmutableList());
        Assert.thatUnchecked(duplicateParameters.isEmpty(), ErrorCode.INVALID_FUNCTION_DEFINITION, () ->
                "unexpected duplicate parameter(s) " + duplicateParameters.stream().map(Object::toString).collect(Collectors.joining(",")));
        return Expressions.of(ctx.sqlParameterDeclaration().stream().map(this::visitSqlParameterDeclaration).collect(ImmutableList.toImmutableList()));
    }

    @Override
    public Expression visitSqlParameterDeclaration(@Nonnull RelationalParser.SqlParameterDeclarationContext ctx) {
        Assert.thatUnchecked(ctx.sqlParameterName != null, "unnamed parameters not supported");
        final var parameterName = visitUid(ctx.sqlParameterName);
        final var parameterType = visitFunctionColumnType(ctx.parameterType);
        final var underlyingType = DataTypeUtils.toRecordLayerType(parameterType);
        Assert.thatUnchecked(parameterType.isResolved());
        Assert.thatUnchecked(ctx.parameterMode() == null || ctx.parameterMode().IN() != null, "only IN parameters are supported");
        if (ctx.DEFAULT() != null) {
            final var defaultExpression = Assert.castUnchecked(visit(ctx.parameterDefault), Expression.class);
            var defaultValue = PromoteValue.inject(defaultExpression.getUnderlying(), underlyingType);
            return Expression.of(defaultValue, parameterName);
        } else {
            return Expression.of(new ThrowsValue(underlyingType), parameterName);
        }
    }

    @Override
    public DataType visitReturnsType(@Nonnull RelationalParser.ReturnsTypeContext ctx) {
        Assert.isNullUnchecked(ctx.returnsTableType(), ErrorCode.UNSUPPORTED_OPERATION,
                "table return type is not supported");
        return lookupType(ctx.columnType().customType, ctx.columnType().primitiveType(), true, ctx.ARRAY() != null);
    }

    @Nonnull
    private DataType lookupType(@Nullable RelationalParser.UidContext customType,
                                @Nullable RelationalParser.PrimitiveTypeContext primitiveTypeContext,
                                boolean isNullable,
                                boolean isRepeated) {
        final SemanticAnalyzer.ParsedTypeInfo typeInfo;
        if (customType != null) {
            final var columnType = visitUid(customType);
            typeInfo = SemanticAnalyzer.ParsedTypeInfo.ofCustomType(columnType, isNullable, isRepeated);
        } else if (primitiveTypeContext != null) {
            typeInfo = SemanticAnalyzer.ParsedTypeInfo.ofPrimitiveType(primitiveTypeContext, isNullable, isRepeated);
        } else {
            throw new UnsupportedOperationException("unsupported type specification");
        }

        return getDelegate().getSemanticAnalyzer().lookupType(typeInfo, metadataBuilder::findType);
    }

    // TODO: remove
    @Nonnull
    @Override
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return ctx.nullNotnull().NOT() == null;
    }

    /**
     * Rewrites a {@code declaredFunction} block inside a {@code DECLARE} block into the equivalent
     * standalone {@code CREATE TEMPORARY FUNCTION ... ON COMMIT DROP FUNCTION AS <body>} statement.
     *
     * <p>The persisted temp-function text on {@code StoredQuery} is required to be byte-identical to
     * what the online path submits, otherwise {@code auxiliaryMetadata} on the query cache key
     * (which fingerprints the {@code normalizedDescription} of each temp routine attached to the
     * schema template) would diverge between warm-up and online, killing cache hits.</p>
     *
     * <p>Pieces are read from the parser context and reassembled &mdash; no surface-syntax
     * manipulation on the DECLARE fragment.</p>
     *
     * <p><b>Case-sensitivity contract with online clients.</b> The keywords emitted here
     * ({@code CREATE TEMPORARY FUNCTION}, {@code ON COMMIT DROP FUNCTION}, {@code AS}) are
     * <em>UPPERCASE</em> by construction. The plan cache key derives its canonical query string
     * from raw keyword tokens (source-preserved by {@code AstNormalizer.visitTerminal}, not
     * uppercased), so a warm-up plan produced from this rewritten text is reachable at runtime
     * <em>only</em> if the online client submits the matching standalone
     * {@code CREATE TEMPORARY FUNCTION ...} DDL with the same uppercase keyword casing. Lowercase
     * or mixed-case keyword submissions from clients will produce a different canonical string and
     * thus a different cache key &mdash; warm-up plans become unreachable. See
     * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/4323">issue #4323</a>
     * for the underlying case-sensitivity behavior of the normalizer.</p>
     */
    @Nonnull
    private static String rewriteDeclaredFunctionToStandalone(@Nonnull final RelationalParser.DeclaredFunctionContext ctx,
                                                              @Nonnull final String sourceText,
                                                              @Nonnull final Set<String> declaredNames) {
        final String name = sliceSource(sourceText, ctx.functionName);
        final String paramList = sliceSource(sourceText, ctx.sqlParameterDeclarationList());
        // Only the body may reference the stored query's signature parameters; scoping the rewrite to the body subtree
        // is what keeps the name and this function's own parameter declarations out of reach.
        final String body = rewriteReferencesToParams(sourceText, ctx.functionBody, declaredNames);
        return "CREATE TEMPORARY FUNCTION " + name + paramList + " ON COMMIT DROP FUNCTION AS " + body;
    }

    /**
     * Returns the identifiers a declared function declares as its own parameters. Resolved through
     * {@link #visitUid} so they are normalized the same way a reference to them in the body would be.
     */
    @Nonnull
    private Set<String> ownParameterNames(@Nonnull final RelationalParser.DeclaredFunctionContext ctx) {
        final var declarations = ctx.sqlParameterDeclarationList().sqlParameterDeclarations();
        if (declarations == null) {
            return Set.of();
        }
        return declarations.sqlParameterDeclaration().stream()
                .map(declaration -> declaration.sqlParameterName)
                .filter(Objects::nonNull)
                .map(uid -> visitUid(uid).getName())
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Checks that the rewritten stored query text still parses. The rewrite is driven by the parse tree so it should
     * always produce valid SQL, but this is what the metadata will carry and what warm-up will re-parse, so a defect
     * here must fail the {@code CREATE} in front of its author rather than become a logged startup failure. The extra
     * parse per stored query is accepted deliberately: DDL is rare, and the alternative is discovering the problem at
     * the next engine start.
     */
    private static void validateRewrittenText(@Nonnull final String storedQueryName,
                                              @Nonnull final String queryString,
                                              @Nonnull final List<String> tempFunctionTexts) {
        try {
            // Both are complete statements — the temp-function text is a `CREATE TEMPORARY FUNCTION ...` exactly as a
            // client would submit it — so both go through the statement entry point.
            for (final var tempFunctionText : tempFunctionTexts) {
                QueryParser.parse(tempFunctionText);
            }
            QueryParser.parse(queryString);
        } catch (final RelationalException | RuntimeException e) {
            throw new RelationalException("Stored query is not valid after resolving its signature parameters",
                    ErrorCode.UNSUPPORTED_QUERY, e)
                    .addContext("storedQueryName", storedQueryName)
                    .toUncheckedWrappedException();
        }
    }

    /**
     * Returns whether a {@code uid} is a plain {@code ID} token, as opposed to a quoted identifier or one of the
     * keywords that {@code simpleId} also accepts. Signature parameter names are restricted to this form so that a
     * reference to one is always spelled the same way as its declaration, and so that a quoted reference cannot
     * accidentally match.
     */
    private static boolean isSimpleIdentifier(@Nonnull final RelationalParser.UidContext ctx) {
        final var simpleId = ctx.simpleId();
        return simpleId != null && simpleId.ID() != null;
    }

    /**
     * Rewrites references to a stored query's signature parameters within {@code fragment} into named parameters
     * {@code ?name}, so the warm-up plan (parsed from this rewritten text) is reachable at runtime when the client
     * re-issues the query binding {@code ?name} by name.
     *
     * <p>What counts as a reference is decided from the parse tree, not from token text: only an identifier used
     * <em>as a value</em> is rewritten, i.e. a {@code fullColumnName} appearing as an {@code expressionAtom} whose
     * {@code fullId} is a single unqualified {@code uid}. That excludes, structurally rather than by special case:
     * <ul>
     * <li>a qualified reference such as {@code t.param} &mdash; its {@code fullId} has more than one {@code uid}, and
     *     it can only ever be a column of {@code t};
     * <li>an alias such as {@code SELECT col AS param} &mdash; the alias hangs off {@code selectElement}, not
     *     {@code expressionAtom};
     * <li>table names, function names, type names and column definitions, none of which are {@code expressionAtom}s.
     * </ul>
     *
     * <p>Matching is <em>case-sensitive</em>: a signature parameter becomes a named prepared parameter (which is always
     * case-sensitive), so a reference must use the declared spelling. Signature parameter names <em>shadow</em> columns
     * of the same (exact) name.
     *
     * @param sourceText the full statement text that {@code fragment} was parsed from
     * @param fragment the subtree to rewrite, e.g. the query body or a declared function's body
     * @param declaredNames the signature parameter names
     * @return the text of {@code fragment} with parameter references rewritten
     */
    @Nonnull
    private static String rewriteReferencesToParams(@Nonnull final String sourceText,
                                                    @Nonnull final ParserRuleContext fragment,
                                                    @Nonnull final Set<String> declaredNames) {
        final int fragmentStart = fragment.getStart().getStartIndex();
        final int fragmentStop = fragment.getStop().getStopIndex() + 1;
        if (declaredNames.isEmpty()) {
            return sourceText.substring(fragmentStart, fragmentStop);
        }
        final List<RelationalParser.UidContext> references = new ArrayList<>();
        collectParameterReferences(fragment, declaredNames, references);
        // Splice by character offset into the original text rather than reconstructing from tokens: the lexer skips
        // whitespace, so rebuilding (e.g. via TokenStreamRewriter) would drop it.
        references.sort(Comparator.comparingInt(uid -> uid.getStart().getStartIndex()));
        final var rewritten = new StringBuilder();
        int copiedUpTo = fragmentStart;
        for (final var reference : references) {
            rewritten.append(sourceText, copiedUpTo, reference.getStart().getStartIndex());
            rewritten.append('?').append(reference.getText());
            copiedUpTo = reference.getStop().getStopIndex() + 1;
        }
        rewritten.append(sourceText, copiedUpTo, fragmentStop);
        return rewritten.toString();
    }

    /**
     * Walks {@code tree} collecting the {@code uid} of every identifier that is used as a value and names one of
     * {@code declaredNames}. See {@link #rewriteReferencesToParams} for why this shape is the one that matters.
     */
    private static void collectParameterReferences(@Nonnull final ParseTree tree,
                                                   @Nonnull final Set<String> declaredNames,
                                                   @Nonnull final List<RelationalParser.UidContext> references) {
        if (tree instanceof RelationalParser.FullColumnNameExpressionAtomContext atom) {
            final var uids = atom.fullColumnName().fullId().uid();
            if (uids.size() == 1 && declaredNames.contains(uids.get(0).getText())) {
                references.add(uids.get(0));
                return;
            }
        }
        for (int i = 0; i < tree.getChildCount(); i++) {
            collectParameterReferences(tree.getChild(i), declaredNames, references);
        }
    }

    @Nonnull
    private static String sliceSource(@Nonnull final String sourceText, @Nonnull final ParserRuleContext ctx) {
        return sourceText.substring(ctx.start.getStartIndex(), ctx.stop.getStopIndex() + 1);
    }
}
