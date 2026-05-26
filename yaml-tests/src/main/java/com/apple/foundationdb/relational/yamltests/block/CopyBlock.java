/*
 * CopyBlock.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A block that performs a cross-cluster COPY operation, exporting data from a source cluster and importing it into a
 * destination cluster. This block manages its own connections (one per cluster) rather than using the single-connection
 * model of {@link ConnectedBlock}.
 *
 * <h2>YAML syntax</h2>
 * <pre>{@code
 * copy_block:
 *   source: { cluster: 0, path: "/FRL/MY_DB" }
 *   dest: { cluster: 1, path: "/FRL/MY_DB" }
 *   export_limit: 2        # optional: setMaxRows on export (enables chunked export with continuations)
 *   import_chunk_size: 1    # optional: split exported data into chunks of this size for import
 * }</pre>
 *
 * <h2>Execution</h2>
 * <ol>
 *     <li><b>Export phase:</b> Connects to the source cluster at the catalog URI and executes
 *         {@code COPY <path> INCREMENT INCARNATION}.
 *         If {@code export_limit} is set, uses {@link RelationalStatement#setMaxRows(int)} and follows continuations
 *         to collect all data.</li>
 *     <li><b>Import phase:</b> Connects to the dest cluster at the catalog URI and executes
 *         {@code COPY <path> FROM ?} with the exported data. If {@code import_chunk_size} is set, partitions the
 *         data into chunks and executes a separate import for each.</li>
 * </ol>
 */
public class CopyBlock extends ReferencedBlock implements Block {

    private static final Logger logger = LogManager.getLogger(CopyBlock.class);

    public static final String COPY_BLOCK = "copy_block";
    private static final URI CATALOG_URI = URI.create("jdbc:embed:/__SYS?schema=CATALOG");

    @Nonnull
    private final YamlExecutionContext executionContext;
    private final CopyInfoForCluster sourceInfo;
    private final CopyInfoForCluster destInfo;

    record CopyInfoForCluster(int cluster, String path, int chunkSize) { }

    private CopyBlock(@Nonnull final YamlReference reference,
                      @Nonnull final CopyInfoForCluster sourceInfo,
                      @Nonnull final CopyInfoForCluster destInfo,
                      @Nonnull final YamlExecutionContext executionContext) {
        super(reference);
        this.sourceInfo = sourceInfo;
        this.destInfo = destInfo;
        this.executionContext = executionContext;
    }

    /**
     * Parses a {@code copy_block} from YAML.
     *
     * @param reference the YAML reference for error reporting
     * @param document the parsed YAML value for this block
     * @param executionContext the execution context
     * @return a singleton list containing the parsed {@link CopyBlock}
     */
    @Nonnull
    public static List<Block> parse(@Nonnull final YamlReference reference, @Nonnull final Object document,
                                    @Nonnull final YamlExecutionContext executionContext) {
        try {
            final Map<?, ?> blockMap = CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(document, COPY_BLOCK));

            return List.of(new CopyBlock(reference,
                    getInfo(blockMap, "source", "export_limit"),
                    getInfo(blockMap, "dest", "import_chunk_size"),
                    executionContext));
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "Error parsing copy_block at " + reference, COPY_BLOCK, reference);
        }
    }

    private static CopyInfoForCluster getInfo(Map<?, ?> map, String name, String chunkSizeName) {
        final Map<?, ?> clusterMap = getMap(map, name);
        return new CopyInfoForCluster(
                getIntOrDefault(clusterMap, "cluster", 0),
                getString(clusterMap, "path", name + " path"),
                getIntOrDefault(map, chunkSizeName, 0));
    }

    @Nonnull
    private static String getString(@Nonnull final Map<?, ?> sourceMap, @Nonnull final String key,
                                    @Nonnull final String description) {
        final String fullDescription = COPY_BLOCK + " " + description;
        return Matchers.notNull(Matchers.string(sourceMap.get(key), fullDescription), fullDescription);
    }

    private static int getIntOrDefault(@Nonnull final Map<?, ?> sourceMap, @Nonnull final String key,
                                       final int defaultValue) {
        return sourceMap.containsKey(key) ? ((Number)sourceMap.get(key)).intValue() : defaultValue;
    }

    @Nonnull
    private static Map<?, ?> getMap(@Nonnull final Map<?, ?> blockMap, @Nonnull final String name) {
        return CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(blockMap.get(name), COPY_BLOCK + " " + name));
    }

    @Override
    public void execute() {
        try {
            logger.debug("📋 Starting copy from {} to {}",
                    sourceInfo, destInfo);
            final List<byte[]> exportedData = executeExport();
            if (logger.isDebugEnabled()) {
                logger.debug("📋 Exported {} record(s) from cluster {}", exportedData.size(), sourceInfo.cluster);
            }
            executeImport(exportedData);
            logger.debug("📋 Copy complete");
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "Error executing copy_block at " + getReference(), COPY_BLOCK, getReference());
        }
    }

    @Nonnull
    private List<byte[]> executeExport() throws SQLException {
        try (YamlConnection conn = executionContext.getConnectionFactory().getNewConnection(CATALOG_URI, sourceInfo.cluster)) {
            final List<byte[]> allData = new ArrayList<>();
            try (RelationalStatement stmt = conn.createStatement()) {
                if (sourceInfo.chunkSize > 0) {
                    stmt.setMaxRows(sourceInfo.chunkSize);
                }
                // Currently this only supports `INCREMENT INCARNATION`, because `PRESERVE INCARNATION` is intended for
                // validating that the copy was done correctly, and thus would need to be a separate thing (either a
                // separate block, or a separate step in this block that validates they are the same after copying)
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + sourceInfo.path  + " INCREMENT INCARNATION")) {
                    Continuation continuation = collectExportBatch(rs, allData);
                    while (sourceInfo.chunkSize > 0 && !continuation.atEnd()) {
                        continuation = executeContinuation(conn, continuation, allData);
                    }

                    Assert.thatUnchecked(continuation.atEnd(), "Should have exhausted continuations");
                }
            }
            return allData;
        }
    }

    @Nonnull
    private Continuation executeContinuation(@Nonnull final YamlConnection conn, @Nonnull final Continuation continuation,
                                             @Nonnull final List<byte[]> allData) throws SQLException {
        try (RelationalPreparedStatement ps = conn.prepareStatement("EXECUTE CONTINUATION ?")) {
            ps.setBytes(1, continuation.serialize());
            // we'll only ever have a continuation if this was limited previously, no need to check chunkSize>0
            ps.setMaxRows(sourceInfo.chunkSize);
            try (RelationalResultSet crs = ps.executeQuery()) {
                return collectExportBatch(crs, allData);
            }
        }
    }

    @Nonnull
    private Continuation collectExportBatch(@Nonnull RelationalResultSet rs,
                                            @Nonnull List<byte[]> allData) throws SQLException {
        final int sizeBefore = allData.size();
        while (rs.next()) {
            allData.add(rs.getBytes(1));
        }
        if (sourceInfo.chunkSize > 0) {
            final int batchSize = allData.size() - sizeBefore;
            if (rs.getContinuation().atEnd()) {
                Assert.thatUnchecked(batchSize <= sourceInfo.chunkSize,
                        "Expected at most " + sourceInfo.chunkSize + " rows from export batch, got " + batchSize);
            } else {
                Assert.thatUnchecked(batchSize == sourceInfo.chunkSize,
                        "Expected " + sourceInfo.chunkSize + " rows from export batch, got " + batchSize);
            }

        }
        return rs.getContinuation();
    }

    private void executeImport(@Nonnull final List<byte[]> data) throws SQLException {
        try (YamlConnection conn = executionContext.getConnectionFactory().getNewConnection(CATALOG_URI, destInfo.cluster)) {
            final List<List<byte[]>> chunks = partition(data);
            int totalCount = 0;
            for (List<byte[]> chunk : chunks) {
                totalCount = importChunk(chunk, conn, totalCount);
            }
            logger.debug("📋 Imported {} record(s) to cluster {}", totalCount, destInfo.cluster);
        }
    }

    private int importChunk(@Nonnull final List<byte[]> chunk, @Nonnull final YamlConnection conn, int totalCount) throws SQLException {
        try (RelationalPreparedStatement ps = conn.prepareStatement("COPY " + destInfo.path + " FROM ?")) {
            Array array = ps.getConnection().createArrayOf("BINARY", chunk.toArray(new byte[0][]));
            ps.setArray(1, array);
            try (RelationalResultSet rs = ps.executeQuery()) {
                Assert.thatUnchecked(rs.next(), "Import should return 1 row");
                final int count = rs.getInt(1);
                Assert.thatUnchecked(count == chunk.size(),
                        "Expected import count " + chunk.size() + ", got " + count);
                totalCount += count;
                Assert.thatUnchecked(!rs.next(), "Import should return exactly 1 row");
            }
        }
        return totalCount;
    }

    @Nonnull
    private List<List<byte[]>> partition(@Nonnull final List<byte[]> data) {
        if (destInfo.chunkSize <= 0 || destInfo.chunkSize >= data.size()) {
            return List.of(data);
        }
        final List<List<byte[]>> chunks = new ArrayList<>();
        for (int i = 0; i < data.size(); i += destInfo.chunkSize) {
            chunks.add(data.subList(i, Math.min(i + destInfo.chunkSize, data.size())));
        }
        return chunks;
    }
}
