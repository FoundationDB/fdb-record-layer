# Notes: fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordContextTest.java
## Visits
- 2026-11-16 65f9c37bc
## Production code
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordContext.java
- fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexStateCommitCheck.java (main user of named commit checks)
## Explored
- Recent FDBRecordContext changes (last 60 days) are javadoc-only; commit check behavior unchanged.
- Commit check added (synchronously) by another commit check during commit: CANDIDATE BUG - the inner check is silently never run and commit succeeds. runCommitChecks streams commitChecks.values() under the map lock; with only one outer check no CME, the new entry is just missed. With 2+ outer checks, the same pattern would likely throw ConcurrentModificationException out of commit (not tested). IndexStateCommitCheck carries a comment working around this ("rather than adding another check").
- runCommitChecks() then commit(): CANDIDATE BUG - checkAsync runs twice (javadoc: "Run any CommitCheckAsyncs that are still outstanding" / "always called once before commit finishes").
- Concurrent addCommitCheck(name, ...) vs removeCommitChecks: CANDIDATE BUG - CME in removeCommitChecks (FDBRecordContext.java:807). Locking is inconsistent: getCommitChecks/removeCommitChecks/removeCommitCheck/addCommitCheck(CompletableFuture) lock `this`; addCommitCheck(CommitCheckAsync)/addCommitCheck(String, ...)/getOrCreateCommitCheck/runCommitChecks lock the map only. Fails 4/4 with 200k adds.
- Transaction id sanitization: covered by TEST_FILE (ascii truncation, non-ascii null).
## Rejected
- runCommitChecks() then commit() runs each check twice: verifier REJECTED. checkAsync is guaranteed to run at commit and may also run earlier; checks are written to tolerate repeated calls, and commit must re-run them.
- A commit check added from inside a running commit check is skipped: verifier REJECTED. Registering checks while commit checks are iterated is not covered by the API contract (javadoc describes checks registered before commit).
- (none yet)
## Untried
- CommitCheck.check() throwing synchronously: commitAsync throws instead of returning a failed future, and the transaction is left open (async failures close it). No javadoc promises either behavior.
- A PostCommit hook that calls addPostCommit/addAfterCommit while running: runPostCommits clears the map in `finally`, so the newly added hook is silently dropped.
- removeCommitCheck (lock `this`, from whenComplete on another thread) racing runCommitChecks (lock map) during commitAsync.
- AfterCommitPostCommit.get uses CompletableFuture.runAsync on the common pool instead of the context executor (loses MDC/executor wrapping, see contextExecutor test).
- reportConflictingKeys + commit failure caused by a commit check: verify getNotCommittedConflictingKeys stays null and no extra read happens on a closed transaction.
- Two outer commit checks where the first adds a third: expect ConcurrentModificationException from commit.
