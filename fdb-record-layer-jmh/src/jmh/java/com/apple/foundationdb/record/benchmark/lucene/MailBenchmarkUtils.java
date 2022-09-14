/*
 * MailBenchmarkUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark.lucene;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestMailProto;
import com.apple.foundationdb.record.lucene.LuceneFunctionNames;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;

import java.time.Duration;
import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

/**
 * Utilities for the javax.mail benchmarks in this package.
 */
public class MailBenchmarkUtils {

    static final String msgStateIndex = "msgStateIdx";

    static String toHumanReadableDuration(Duration duration) {
        return String.format("%d:%d.%d[%d]", duration.toMinutes(), duration.toSeconds(), duration.toMillis(), duration.getNano() / 1000);
    }

    static RecordMetaData recordStoreMetaData() {
        RecordMetaDataBuilder rmd = RecordMetaData.newBuilder().setRecords(TestMailProto.getDescriptor());
        rmd.setSplitLongRecords(true);
        final RecordTypeBuilder msgStateBuilder = rmd.getRecordType("MessageState");
        msgStateBuilder.setPrimaryKey(Key.Expressions.field("uid"));

        Index msgStateMboxRefTextSearchRef = new Index(msgStateIndex,
                Key.Expressions.concat(
                        Key.Expressions.field("mBoxRef"),
                        Key.Expressions.field("textSearchRef"),
                        Key.Expressions.field("uid")),
                IndexTypes.VALUE
        );
        rmd.addIndex("MessageState", msgStateMboxRefTextSearchRef);


        Index idx = new Index("fullSearchIndex",
                Key.Expressions.concat(
                        function(LuceneFunctionNames.LUCENE_TEXT, field("searchBody")),
                        function(LuceneFunctionNames.LUCENE_TEXT, field("headerBlob"))
                ),
                LuceneIndexTypes.LUCENE,
                Map.of(
                        "luceneAnalyzerName", "SYNONYM_MAIL",
                        "luceneAnalyzerNamePerField", "header_subject:NGRAM,header_from:NGRAM,header_to:NGRAM,header_bcc:NGRAM",
                        "autoCompleteEnabled", "true",
                        "autoCompleteExcludedFields", "headerBlob"
                ));

        rmd.getRecordType("TextSearch").setPrimaryKey(Key.Expressions.field("___recordID"));
        rmd.addIndex("TextSearch", idx);

        return rmd.build();
    }

    static RecordMetaData mboxInLuceneMetaData(){
        RecordMetaDataBuilder rmd = RecordMetaData.newBuilder().setRecords(TestMailProto.getDescriptor());
        rmd.setSplitLongRecords(true);
        final RecordTypeBuilder msgStateBuilder = rmd.getRecordType("MessageState");
        msgStateBuilder.setPrimaryKey(Key.Expressions.field("uid"));

        Index msgStateMboxRefTextSearchRef = new Index(msgStateIndex,
                Key.Expressions.concat(
                        Key.Expressions.field("mBoxRef"),
                        Key.Expressions.field("textSearchRef"),
                        Key.Expressions.field("uid")),
                IndexTypes.VALUE
        );
        rmd.addIndex("MessageState", msgStateMboxRefTextSearchRef);

        Index idx = new Index("fullSearchIndex",
                Key.Expressions.concat(
                        function(LuceneFunctionNames.LUCENE_TEXT, field("searchBody")),
                        function(LuceneFunctionNames.LUCENE_TEXT, field("headerBlob")),
                        function(LuceneFunctionNames.LUCENE_TEXT, field("mBoxRef")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("msgStateUid"))
                ),
                LuceneIndexTypes.LUCENE,
                Map.of(
                        "luceneAnalyzerName", "SYNONYM_MAIL",
                        "luceneAnalyzerNamePerField", "header_subject:NGRAM,header_from:NGRAM,header_to:NGRAM,header_bcc:NGRAM",
                        "autoCompleteEnabled", "true",
                        "autoCompleteExcludedFields", "headerBlob"
                ));

        rmd.getRecordType("TextSearch").setPrimaryKey(Key.Expressions.field("___recordID"));
        rmd.addIndex("TextSearch", idx);

        return rmd.build();
    }

    static RecordMetaData joinedRecordTypeMetaData() {
        RecordMetaDataBuilder rmd = RecordMetaData.newBuilder().setRecords(TestMailProto.getDescriptor());
        rmd.setSplitLongRecords(true);
        final RecordTypeBuilder msgStateBuilder = rmd.getRecordType("MessageState");
        msgStateBuilder.setPrimaryKey(Key.Expressions.field("uid"));
        final RecordTypeBuilder textSearchBuilder = rmd.getRecordType("TextSearch");
        textSearchBuilder.setPrimaryKey(Key.Expressions.field("___recordID"));

        JoinedRecordTypeBuilder joined = rmd.addJoinedRecordType("TextSearchMessageState");
        joined.addConstituent(msgStateBuilder);
        joined.addConstituent(textSearchBuilder);
        joined.addJoin("MessageState", "textSearchRef", "TextSearch", "___recordID");

        Index idx = new Index("joinedSearchIndex",
                Key.Expressions.concat(
                        field("MessageState").nest(Key.Expressions.concatenateFields("uid", "mBoxRef")),
                        field("TextSearch").nest(Key.Expressions.concat(
                                function(LuceneFunctionNames.LUCENE_TEXT, field("searchBody")),
                                function(LuceneFunctionNames.LUCENE_TEXT, field("headerBlob"))
                        ))
                ),
                LuceneIndexTypes.LUCENE,
                Map.of(
                        "luceneAnalyzerName", "SYNONYM_MAIL",
                        "luceneAnalyzerNamePerField", "header_subject:NGRAM,header_from:NGRAM,header_to:NGRAM,header_bcc:NGRAM",
                        "autoCompleteEnabled", "true",
                        "autoCompleteExcludedFields", "headerBlob"
                ));
        rmd.addIndex("TextSearchMessageState", idx);
        return rmd.build();
    }
}

