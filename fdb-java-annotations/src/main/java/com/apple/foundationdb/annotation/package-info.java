/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
 * Annotations defined for use with FoundationDB. Currently, this includes:
 *
 * <ul>
 *     <li> {@link com.apple.foundationdb.annotation.API API} for specifying API stability levels.</li>
 *     <li> {@link com.apple.foundationdb.annotation.SpotBugsSuppressWarnings SpotBugsSuppressWarnings} for suppressing
 *         static analysis warnings.</li>
 *     <li> {@link com.apple.foundationdb.annotation.GenerateVisitor GenerateVisitor} for automatically generating visitor
 *         interfaces of class hierarchies</li>
 * </ul>
 *
 * <p>
 * None of the annotations have dependencies on
 * </p>
 */
package com.apple.foundationdb.annotation;
