/*
 * Clusters.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.connectionfactory;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An ordered, immutable collection of cluster entries, each pairing a server (or driver) of type {@code T}
 * with the cluster file it is connected to. Each server (or driver) must be identical, except that it is pointing to a
 * different cluster file. Must contain at least one entry.
 *
 * @param <T> the type of server or driver held by each entry
 */
public class Clusters<T extends Clusters.BoundToCluster> implements Iterable<T> {
    @Nonnull
    private final List<T> entries;

    /** Private constructor to support the static {@link Clusters#empty()}. */
    private Clusters() {
        this.entries = List.of();
    }

    private Clusters(@Nonnull List<T> entries) {
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("At least one cluster entry is required");
        }
        this.entries = List.copyOf(entries);
    }

    public static <T extends BoundToCluster> Clusters<T> empty() {
        return new Clusters<>();
    }

    public static <T extends BoundToCluster> Clusters<T> fromClusterFiles(List<String> clusterFiles, Function<String, T> toServer) {
        return new Clusters<>(clusterFiles.stream()
                .map(toServer)
                .collect(Collectors.toList()));
    }

    public static <V> Clusters<Clusters.Entry<V>> fromClusterFilesAsEntries(List<String> clusterFiles, Function<String, V> toServer) {
        return fromClusterFiles(clusterFiles,
                clusterFile -> new Clusters.Entry<>(toServer.apply(clusterFile), clusterFile));
    }

    public <R extends BoundToCluster> Clusters<R> map(Function<T, R> mapper) {
        return new Clusters<>(entries.stream()
                .map(mapper)
                .collect(Collectors.toList()));
    }

    public static <K, V> Entry<V> mapEntry(Clusters.Entry<K> existing, Function<K, V> mapper) {
        return new Clusters.Entry<V>(mapper.apply(existing.server), existing.clusterFile);
    }

    /**
     * Return a piece of information about the underlying connections.
     * @param getter a function to extract information that should be the same for all clusters (since they are identical)
     * @param <R> the type of the extracted information.
     * @return the information
     */
    public <R> R getInfo(Function<T, R> getter) {
        return entries.stream().map(getter)
                .findFirst()
                .orElseThrow(() -> new IndexOutOfBoundsException("No Clusters found"));
    }

    /**
     * Returns the cluster files in order.
     */
    @Nonnull
    public List<String> clusterFiles() {
        return entries.stream().map(BoundToCluster::clusterFile).collect(Collectors.toList());
    }

    /**
     * Returns the number of clusters.
     */
    public int size() {
        return entries.size();
    }

    /**
     * Returns the entry at the given cluster index, after bounds-checking.
     *
     * @param clusterIndex the zero-based cluster index
     * @return the entry at that index
     * @throws SQLException if the index is out of range
     */
    @Nonnull
    public T get(int clusterIndex) throws SQLException {
        if (clusterIndex < 0 || clusterIndex >= entries.size()) {
            throw new SQLException("Cluster index " + clusterIndex + " not available (only " +
                    entries.size() + " clusters configured)");
        }
        return entries.get(clusterIndex);
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        return entries.iterator();
    }

    /**
     * A server (or driver) paired with its cluster file.
     *
     * @param <T> the type of server or driver
     */
    public static class Entry<T> implements BoundToCluster {
        @Nonnull
        private final T server;
        @Nonnull
        private final String clusterFile;

        public Entry(@Nonnull T server, @Nonnull String clusterFile) {
            this.server = server;
            this.clusterFile = clusterFile;
        }

        @Nonnull
        public T server() {
            return server;
        }

        @Nonnull
        @Override
        public String clusterFile() {
            return clusterFile;
        }
    }

    /**
     * An interface for a server (or driver) and its associated cluster file.
     */
    public interface BoundToCluster {
        @Nonnull
        String clusterFile();
    }
}
