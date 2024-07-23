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

import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata information about a lucene index, in response to {@link LuceneGetMetadataInfo}.
 */
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
        private final Collection<String> files;
        private final int fieldInfoCount;

        public LuceneInfo(final int documentCount, final Collection<String> files, final int fieldInfoCount) {
            this.documentCount = documentCount;
            this.files = files;
            this.fieldInfoCount = fieldInfoCount;
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

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final LuceneInfo that = (LuceneInfo)o;
            return documentCount == that.documentCount && fieldInfoCount == that.fieldInfoCount && Objects.equals(files, that.files);
        }

        @Override
        public int hashCode() {
            return Objects.hash(documentCount, files, fieldInfoCount);
        }
    }
}
