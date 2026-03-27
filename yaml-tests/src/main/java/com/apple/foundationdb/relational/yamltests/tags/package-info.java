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
 * Custom YAML tags for the YAMSQL test framework, discovered automatically via
 * {@code @AutoService(CustomTag.class)}.
 *
 * <p>Tags are used in two contexts:
 * <ol>
 *   <li><b>Result matching</b> (in {@code result}/{@code unorderedResult} blocks): tags that implement
 *       {@link com.apple.foundationdb.relational.yamltests.tags.Matchable Matchable} compare expected vs actual
 *       values.</li>
 *   <li><b>Parameter injection</b> (inside {@code !! ... !!} delimiters in queries): tags are resolved at parse
 *       time and injected into the SQL. See
 *       {@link com.apple.foundationdb.relational.yamltests.command.QueryInterpreter QueryInterpreter}.</li>
 * </ol>
 *
 * <h3>Available tags</h3>
 * <table>
 *   <caption>Custom YAML tags</caption>
 *   <tr><th>Tag</th><th>Syntax</th><th>Context</th><th>Description</th></tr>
 *   <tr><td>{@code !l}</td><td>{@code !l 42}</td><td>Both</td>
 *       <td>Long literal. Required for matching {@code BIGINT} columns (YAML parses bare numbers as Integer).</td></tr>
 *   <tr><td>{@code !ignore}</td><td>{@code !ignore _}</td><td>Result</td>
 *       <td>Matches any value (skip this cell).</td></tr>
 *   <tr><td>{@code !null}</td><td>{@code !null _}</td><td>Result</td>
 *       <td>Matches only {@code NULL}.</td></tr>
 *   <tr><td>{@code !not_null}</td><td>{@code !not_null _}</td><td>Result</td>
 *       <td>Matches any non-{@code NULL} value.</td></tr>
 *   <tr><td>{@code !sc}</td><td>{@code !sc "substring"}</td><td>Result</td>
 *       <td>Matches strings containing the given substring.</td></tr>
 *   <tr><td>{@code !uuid}</td><td>{@code !uuid "550e8400-..."}</td><td>Both</td>
 *       <td>UUID value.</td></tr>
 *   <tr><td>{@code !v16}, {@code !v32}, {@code !v64}</td><td>{@code !v32 [1.0, 2.0]}</td><td>Both</td>
 *       <td>Vector at 16/32/64-bit precision.</td></tr>
 *   <tr><td>{@code !randomStr}</td><td>{@code !randomStr seed 99 length 1000}</td><td>Both</td>
 *       <td>Deterministic random alphanumeric string (chars: a-z, 0-9).</td></tr>
 *   <tr><td>{@code !pos}</td><td>{@code !pos 1}</td><td>Result</td>
 *       <td>Reference column by 1-based position: {@code {!pos 1: value}}.</td></tr>
 *   <tr><td>{@code !current_version}</td><td>{@code !current_version}</td><td>Options</td>
 *       <td>Resolves to the current semantic version (for {@code supported_version}).</td></tr>
 * </table>
 *
 * <p>To add a new tag, create a class implementing {@link com.apple.foundationdb.relational.yamltests.tags.CustomTag}
 * with {@code @AutoService(CustomTag.class)}. See existing tags in this package for examples.
 */

package com.apple.foundationdb.relational.yamltests.tags;
