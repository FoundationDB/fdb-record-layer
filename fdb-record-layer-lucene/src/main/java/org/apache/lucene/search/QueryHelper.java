/*
 * QueryHelper.java
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

package org.apache.lucene.search;

import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import java.util.Collections;
import java.util.List;

/**
 * The goal of this Helper class is to extract terms from queries.  It is located in the private package because some
 * query implementations are private.
 *
 */
public class QueryHelper {
    /**
     * Class to get optional terms for a query.
     *
     * @param query query implemenation
     * @return List of terms
     */
    public static List<Term> getQueriesTerms(Query query) {
        if (query instanceof PhraseQuery) {
            return Lists.newArrayList(((PhraseQuery)query).getTerms());
        } else if (query instanceof SynonymQuery) {
            return ((SynonymQuery)query).getTerms();
        } else if (query instanceof TermQuery) {
            return Lists.newArrayList(((TermQuery)query).getTerm());
        } else if (query instanceof BoostQuery) {
            return getQueriesTerms(((BoostQuery)query).getQuery());
        } else if (query instanceof MultiTermQueryConstantScoreWrapper) {
            return getQueriesTerms(((MultiTermQueryConstantScoreWrapper)query).getQuery());
        } else {
            return Collections.emptyList();
        }
    }

}
