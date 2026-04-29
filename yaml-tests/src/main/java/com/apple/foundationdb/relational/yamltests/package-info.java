/*
 * package-info.java
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

/**
 * YAMSQL (YAML SQL) test framework for writing declarative SQL integration tests.
 *
 * <p>Tests are authored as {@code .yamsql} files in {@code yaml-tests/src/test/resources/}. Each file is a
 * multi-document YAML file (documents separated by {@code ---}, file ends with {@code ...}). Each document is
 * parsed into a {@link com.apple.foundationdb.relational.yamltests.block.Block Block} and executed sequentially
 * by {@link com.apple.foundationdb.relational.yamltests.YamlRunner YamlRunner}.
 *
 * <p>See {@code showcasing-tests.yamsql} for a comprehensive, runnable reference of all supported syntax and features.
 *
 * <h2>Key entry points</h2>
 * <ul>
 *   <li>{@link com.apple.foundationdb.relational.yamltests.block.Block Block} — block type dispatch and the full
 *       list of supported block types ({@code options}, {@code schema_template}, {@code setup}, {@code test_block},
 *       {@code include}, {@code transaction_setups}).</li>
 *   <li>{@link com.apple.foundationdb.relational.yamltests.block.TestBlock TestBlock} — test execution options
 *       (mode, repetition, presets, connection lifecycle, statement type).</li>
 *   <li>{@link com.apple.foundationdb.relational.yamltests.command.QueryConfig QueryConfig} — all query-level
 *       configuration directives ({@code result}, {@code error}, {@code explain}, {@code count}, etc.).</li>
 *   <li>{@link com.apple.foundationdb.relational.yamltests.command.QueryInterpreter QueryInterpreter} — parameter
 *       injection syntax ({@code !! ... !!} delimiters).</li>
 *   <li>{@link com.apple.foundationdb.relational.yamltests.tags} package — custom YAML tags
 *       ({@code !l}, {@code !ignore}, {@code !null}, {@code !randomStr}, etc.).</li>
 * </ul>
 *
 * <h2>Adding new features</h2>
 * <ul>
 *   <li><b>New custom tag:</b> implement {@code CustomTag} with {@code @AutoService(CustomTag.class)} in the
 *       {@link com.apple.foundationdb.relational.yamltests.tags tags} package.</li>
 *   <li><b>New query config directive:</b> add a constant and factory method in
 *       {@link com.apple.foundationdb.relational.yamltests.command.QueryConfig QueryConfig}, register in its
 *       {@code parseConfig} dispatch, and handle in
 *       {@link com.apple.foundationdb.relational.yamltests.command.QueryCommand QueryCommand}'s
 *       {@code executeInternal}.</li>
 *   <li><b>New block type:</b> create a class implementing
 *       {@link com.apple.foundationdb.relational.yamltests.block.Block Block} and register its key in
 *       {@code Block.parse()}.</li>
 * </ul>
 */

package com.apple.foundationdb.relational.yamltests;
