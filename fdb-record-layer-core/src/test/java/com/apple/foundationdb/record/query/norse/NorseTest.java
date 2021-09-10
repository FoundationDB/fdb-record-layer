/*
 * NorseTest.java
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

package com.apple.foundationdb.record.query.norse;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class NorseTest {

    @Test
    void testSimpleStatement() {
        final ANTLRInputStream in = new ANTLRInputStream("x < 5");
        NorseLexer lexer = new NorseLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NorseParser parser = new NorseParser(tokens);
        final ParseTree tree = parser.pipe();

        final NorseParserVisitorImpl visitor = new NorseParserVisitorImpl();
        Object o = visitor.visit(tree);
        System.out.println(o);
    }
}
