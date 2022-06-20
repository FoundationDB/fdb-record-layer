/*
 * RecordQueryPlanWithMatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A query plan that was created using a {@link MatchCandidate} to match a physical access path.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordQueryPlanWithMatchCandidate extends RecordQueryPlan {
    @Nonnull
    Optional<? extends MatchCandidate> getMatchCandidateMaybe();

    default MatchCandidate getMatchCandidate() {
        return getMatchCandidateMaybe().orElseThrow(() -> new RecordCoreException("called from a context where the existence of a match candidate is expected."));
    }
}
