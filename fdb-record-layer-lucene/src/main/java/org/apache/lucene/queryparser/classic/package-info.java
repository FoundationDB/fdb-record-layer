/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
 * This package is copied from Lucene main branch (cce33b0) to achieve the following goals:
 * <ul>
 *     <li>more fine-grained control of fields of certain types w.r.t. used analyzers</li>
 *     <li>extend the parser to support new features such as bitset indexing</li>
 *     <li>have more control over parsing workflow, and be more explicit about error messages, if possible</li>
 * </ul>
 *
 * Note to devs: in this package, most of the files are auto-generated as a side effect of {@code compileJavacc} Gradle
 * task. The only files which are not auto-generated are FastCharStream.java, QueryParser.jj, and QueryParserBase.java.
 */
package org.apache.lucene.queryparser.classic;
