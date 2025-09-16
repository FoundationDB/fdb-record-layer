/*
 * LuceneMetadataInfo.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Metadata information about a lucene index, in response to {@link LuceneGetMetadataInfo}.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneMetadataInfo extends IndexOperationResult {
    private List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfo;
    private Map<Integer, LuceneInfo> luceneInfo;

    public LuceneMetadataInfo(@Nonnull final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfo,
                              @Nonnull final Map<Integer, LuceneInfo> luceneInfo) {
        this.partitionInfo = partitionInfo;
        this.luceneInfo = luceneInfo;
    }

    /**
     * List of the metadata information for a given partition, most-recent will be first.
     * @return the list of metadata information for all the partitions
     */
    public List<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionInfo() {
        return partitionInfo;
    }

    /**
     * Map from partition id to information from lucene.
     * @return the information from lucene for all partitions, or the one partition request if that was requested
     */
    public Map<Integer, LuceneInfo> getLuceneInfo() {
        return luceneInfo;
    }

    /**
     * Information about an individual Lucene directory.
     */
    public static class LuceneInfo {
        private final int documentCount;
        @API(API.Status.DEPRECATED) // replaced by the detailedFileInfos field
        private final Collection<String> files;
        private final int fieldInfoCount;
        @Nullable
        private final List<LuceneFileInfo> detailedFileInfos;

        @API(API.Status.DEPRECATED)
        public LuceneInfo(final int documentCount, final Collection<String> files, final int fieldInfoCount) {
            this.documentCount = documentCount;
            this.files = files;
            this.fieldInfoCount = fieldInfoCount;
            this.detailedFileInfos = null;
        }

        public LuceneInfo(final int documentCount,
                          final int fieldInfoCount,
                          @Nonnull final List<LuceneFileInfo> detailedFileInfos) {
            this.documentCount = documentCount;
            this.files = detailedFileInfos.stream().map(LuceneFileInfo::getName).collect(Collectors.toList());
            this.fieldInfoCount = fieldInfoCount;
            this.detailedFileInfos = Collections.unmodifiableList(detailedFileInfos);
        }

        /**
         * The number of documents that lucene thinks are in the index.
         * @return the number of documents
         */
        public int getDocumentCount() {
            return documentCount;
        }

        /**
         * The file listing from the directory.
         *
         * @return the list of files in the directory
         */
        @API(API.Status.DEPRECATED) // Use the getDetailedFileInfos instead
        public Collection<String> getFiles() {
            return files;
        }

        /**
         * The number of FieldInfos in the directory; see {@link com.apple.foundationdb.record.lucene.directory.FieldInfosStorage}.
         * @return the number of field infos in the directory
         */
        public int getFieldInfoCount() {
            return fieldInfoCount;
        }

        /**
         * The detailed file info from the directory.
         * (Optional) - if the request did not specify that this is needed, this would be null.
         * Note: This method returns the internal collection. It should not be modified.
         * @return the list of fileInfos in the directory
         */
        @Nullable
        public List<LuceneFileInfo> getDetailedFileInfos() {
            return detailedFileInfos;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final LuceneInfo that = (LuceneInfo)o;
            return documentCount == that.documentCount &&
                    fieldInfoCount == that.fieldInfoCount &&
                    Objects.equals(files, that.files) &&
                    Objects.equals(detailedFileInfos, that.detailedFileInfos);
        }

        @Override
        public int hashCode() {
            return Objects.hash(documentCount, files, fieldInfoCount, detailedFileInfos);
        }
    }

    /**
     * Detailed file info class for the case where the response contains this information.
     */
    public static class LuceneFileInfo {
        private final String name;
        private final long id;
        private final long size;

        public LuceneFileInfo(final String name, final long id, final long size) {
            this.name = name;
            this.id = id;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public long getId() {
            return id;
        }

        public long getSize() {
            return size;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LuceneFileInfo)) {
                return false;
            }
            final LuceneFileInfo that = (LuceneFileInfo)o;
            return id == that.id && size == that.size && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, id, size);
        }
    }
}
