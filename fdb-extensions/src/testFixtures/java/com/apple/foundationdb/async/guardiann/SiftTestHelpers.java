/*
 * SiftTestHelpers.java
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

package com.apple.foundationdb.async.guardiann;

/**
 * SIFT-specific test fixtures: the on-disk dataset paths, bound to the dataset-agnostic loaders in
 * {@link VecsDatasetLoaders}. Anything here that has a "Sift" in its name is just a SIFT-bound path constant fed to a
 * generic {@code VecsDatasetLoaders} loader; tests that work on another dataset should call the generic
 * {@code VecsDatasetLoaders} loaders directly with their own {@code .fvecs}/{@code .ivecs} paths. Lives in
 * {@code testFixtures} so both the fdb-extensions guardiann tests and the record-layer's vector-index tests can share
 * the same dataset locations.
 */
public class SiftTestHelpers {
    /** Path to the SIFT-small base vectors {@code .fvecs} file (10k × 128). Produced by the
     *  gradle {@code extractSiftSmall} task. */
    public static final String SIFT_SMALL_BASE_PATH = ".out/extracted/siftsmall/siftsmall_base.fvecs";

    /** Path to the SIFT-small query vectors {@code .fvecs} file (100 × 128). */
    public static final String SIFT_SMALL_QUERY_PATH = ".out/extracted/siftsmall/siftsmall_query.fvecs";

    /** Path to the SIFT-small ground-truth top-k indices {@code .ivecs} file. */
    public static final String SIFT_SMALL_GROUNDTRUTH_PATH = ".out/extracted/siftsmall/siftsmall_groundtruth.ivecs";

    /** Path to the SIFT-1M base vectors {@code .fvecs} file (1M × 128). Downloaded by gradle to
     *  {@code .out/downloads/sift_base.fvecs}. */
    public static final String SIFT_1M_BASE_PATH = ".out/downloads/sift_base.fvecs";

    /** Path to the SIFT-1M query vectors {@code .fvecs} file (10k × 128). */
    public static final String SIFT_1M_QUERY_PATH = ".out/downloads/sift_query.fvecs";

    /** Path to the SIFT-1M ground-truth top-k indices {@code .ivecs} file. */
    public static final String SIFT_1M_GROUNDTRUTH_PATH = ".out/downloads/sift_groundtruth.ivecs";

    private SiftTestHelpers() {
    }
}
