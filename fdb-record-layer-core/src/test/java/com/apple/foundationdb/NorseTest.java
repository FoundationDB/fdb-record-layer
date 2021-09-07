/*
<<<<<<< Updated upstream:fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/plans/QueryResultElement.java
 * QueryResultElement.java
=======
 * NorseTest.java
>>>>>>> Stashed changes:fdb-record-layer-core/src/test/java/com/apple/foundationdb/NorseTest.java
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

package com.apple.foundationdb.record.query.plan.plans;

<<<<<<< Updated upstream:fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/plan/plans/QueryResultElement.java
/**
 * Marker interface for the elements that can be stored in a {@link QueryResult}.
 */
public interface QueryResultElement {
=======
import com.apple.foundationdb.norse.NorseLexer;
import com.apple.foundationdb.norse.NorseParser;
import com.apple.foundationdb.norse.NorseParserBaseVisitor;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class NorseTest {

    public static class NorseParserVisitor extends NorseParserBaseVisitor<Void> {

    }

    @Test
    void testSimpleStatement() {
        final ANTLRInputStream in = new ANTLRInputStream("read(\"a\") | filter _ = 2");
        NorseLexer lexer = new NorseLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NorseParser parser = new NorseParser(tokens);
        final ParseTree tree = parser.pipe();

        final NorseParserVisitor visitor = new NorseParserVisitor();
        visitor.visit(tree);
    }
>>>>>>> Stashed changes:fdb-record-layer-core/src/test/java/com/apple/foundationdb/NorseTest.java
}
