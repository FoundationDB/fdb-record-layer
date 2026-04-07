/*
 * CopyBlock.java
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
 *     <li><b>Export phase:</b> Connects to the source cluster at the catalog URI and executes {@code COPY <path>}.
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

    private final int sourceCluster;
    @Nonnull
    private final String sourcePath;
    private final int destCluster;
    @Nonnull
    private final String destPath;
    private final int exportLimit;
    private final int importChunkSize;
    @Nonnull
    private final YamlExecutionContext executionContext;

    private CopyBlock(@Nonnull YamlReference reference,
                      int sourceCluster, @Nonnull String sourcePath,
                      int destCluster, @Nonnull String destPath,
                      int exportLimit, int importChunkSize,
                      @Nonnull YamlExecutionContext executionContext) {
        super(reference);
        this.sourceCluster = sourceCluster;
        this.sourcePath = sourcePath;
        this.destCluster = destCluster;
        this.destPath = destPath;
        this.exportLimit = exportLimit;
        this.importChunkSize = importChunkSize;
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
    public static List<Block> parse(@Nonnull YamlReference reference, @Nonnull Object document,
                                    @Nonnull YamlExecutionContext executionContext) {
        try {
            final Map<?, ?> blockMap = CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(document, COPY_BLOCK));
            final Map<?, ?> sourceMap = getMap(blockMap, "source");
            final int sourceCluster = getIntOrDefault(sourceMap, "cluster", 0);
            final String sourcePath = getString(sourceMap, "path", "source path");

            final Map<?, ?> destMap = getMap(blockMap, "dest");
            final int destCluster = getIntOrDefault(destMap, "cluster", 0);
            final String destPath = getString(destMap, "path", "dest path");

            final int exportLimit = getIntOrDefault(blockMap, "export_limit", 0);
            final int importChunkSize = getIntOrDefault(blockMap, "import_chunk_size", 0);

            return List.of(new CopyBlock(reference, sourceCluster, sourcePath, destCluster, destPath,
                    exportLimit, importChunkSize, executionContext));
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "Error parsing copy_block at " + reference, COPY_BLOCK, reference);
        }
    }

    @Nonnull
    private static String getString(final Map<?, ?> sourceMap, String key, String description) {
        final String fullDescription = "copy_block " + description;
        return Matchers.notNull(Matchers.string(sourceMap.get(key), fullDescription), fullDescription);
    }

    private static int getIntOrDefault(final Map<?, ?> sourceMap, String key, int defaultValue) {
        return sourceMap.containsKey(key) ? ((Number)sourceMap.get(key)).intValue() : defaultValue;
    }

    private static Map<?, ?> getMap(final Map<?, ?> blockMap, String name) {
        return CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(blockMap.get(name), "copy_block " + name));
    }

    @Override
    public void execute() {
        try {
            logger.debug("📋 Starting copy from cluster {} ({}) to cluster {} ({})",
                    sourceCluster, sourcePath, destCluster, destPath);
            final List<byte[]> exportedData = executeExport();
            if (logger.isDebugEnabled()) {
                logger.debug("📋 Exported {} record(s) from cluster {}", exportedData.size(), sourceCluster);
            }
            executeImport(exportedData);
            logger.debug("📋 Copy complete");
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "Error executing copy_block at " + getReference(), COPY_BLOCK, getReference());
        }
    }

    private List<byte[]> executeExport() throws SQLException {
        try (YamlConnection conn = executionContext.getConnectionFactory().getNewConnection(CATALOG_URI, sourceCluster)) {
            final List<byte[]> allData = new ArrayList<>();
            try (RelationalStatement stmt = conn.createStatement()) {
                if (exportLimit > 0) {
                    stmt.setMaxRows(exportLimit);
                }
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + sourcePath)) {
                    while (rs.next()) {
                        allData.add(rs.getBytes(1));
                    }
                    if (exportLimit > 0) {
                        Assert.thatUnchecked(allData.size() <= exportLimit,
                                "Expected at most " + exportLimit + " rows from initial export batch, got " + allData.size());
                        Continuation continuation = rs.getContinuation();
                        while (!continuation.atEnd()) {
                            final int sizeBefore = allData.size();
                            try (RelationalPreparedStatement ps = conn.prepareStatement("EXECUTE CONTINUATION ?")) {
                                ps.setBytes(1, continuation.serialize());
                                ps.setMaxRows(exportLimit);
                                try (RelationalResultSet crs = ps.executeQuery()) {
                                    while (crs.next()) {
                                        allData.add(crs.getBytes(1));
                                    }
                                    continuation = crs.getContinuation();
                                }
                            }
                            final int batchSize = allData.size() - sizeBefore;
                            Assert.thatUnchecked(batchSize <= exportLimit,
                                    "Expected at most " + exportLimit + " rows from continuation batch, got " + batchSize);
                        }
                    }
                }
            }
            return allData;
        }
    }

    private void executeImport(@Nonnull List<byte[]> data) throws SQLException {
        try (YamlConnection conn = executionContext.getConnectionFactory().getNewConnection(CATALOG_URI, destCluster)) {
            final List<List<byte[]>> chunks = partition(data);
            int totalCount = 0;
            for (List<byte[]> chunk : chunks) {
                try (RelationalPreparedStatement ps = conn.prepareStatement("COPY " + destPath + " FROM ?")) {
                    java.sql.Array array = ps.getConnection().createArrayOf("BINARY", chunk.toArray(new byte[0][]));
                    ps.setArray(1, array);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            final int count = rs.getInt(1);
                            Assert.thatUnchecked(count == chunk.size(),
                                    "Expected import count " + chunk.size() + ", got " + count);
                            totalCount += count;
                        }
                    }
                }
            }
            logger.debug("📋 Imported {} record(s) to cluster {}", totalCount, destCluster);
        }
    }

    @Nonnull
    private List<List<byte[]>> partition(@Nonnull List<byte[]> data) {
        if (importChunkSize <= 0 || importChunkSize >= data.size()) {
            return List.of(data);
        }
        final List<List<byte[]>> chunks = new ArrayList<>();
        for (int i = 0; i < data.size(); i += importChunkSize) {
            chunks.add(data.subList(i, Math.min(i + importChunkSize, data.size())));
        }
        return chunks;
    }
}
