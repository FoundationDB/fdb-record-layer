# Index Maintainer Scenario Testing Framework

A small, extensible harness for exercising **index maintainers** against a shared battery of
**scenarios** (rebuild, write-only maintenance, snapshot isolation, group deletion, synthetic
record types, …). Each index type is described **once** by a tiny `IndexDefinition`; the framework
owns everything else — record metadata, record generation, primary keys, grouping, and
synthetic-type wiring — and runs every scenario against every index type automatically.

> **Why:** maintainer behaviour is easy to get subtly wrong and hard to cover uniformly. This turns
> "does index type *X* behave correctly under scenario *Y*?" into a matrix that fills in by itself:
> a new index type is tested by all existing scenarios, and a new scenario runs against all existing
> index types — with no cross-wiring.

---

## Architecture

Two responsibilities are cleanly split:

- **The definition** says *what* to index and *how* to scan it — nothing about storage, records, or
  scenarios.
- **The framework** owns *how* records are shaped and stored and *which* scenarios run, so those
  concerns evolve independently of the index types.

Scenarios are discovered as `ServiceLoader` plugins, so a test class never names them; it only wires
in its definition:

```java
@ParameterizedTest
@IndexScenarios // scenarios injected by the framework
void indexScenariosTest(IndexScenario scenario) throws Exception {
    scenario.runTest(
            // the index type under test
            XxxIndexDefinition::new,
            // how to open a transaction
            () -> database.openContext(config),
            // an appropriate store builder, notably does not set the metadata
            FDBRecordStore.newBuilder().setKeySpacePath(path));
}
```

> **Note:** The arguments for how to open a store or context are mostly so that this can fit well
> into existing test classes which have different ways of specifying the context/path.

---

## The shared schema + `IndexTarget`

Rather than a bespoke record type per index, the framework uses **one** record type whose indexable
content is a **generic sub-message**. A definition fills in only the field(s) it cares about; the
framework owns the primary key and the grouping field.

The definition writes its value expression **relative to that sub-message**, and an `IndexTarget`
roots it at wherever the sub-message actually lives for the current run. That single indirection is
what lets one `buildIndex` implementation serve normal, grouped, and synthetic (joined/unnested)
runs — so synthetic-type coverage comes essentially for free for every index type.

Where a maintainer genuinely cannot support a scenario, its definition opts out via a capability
flag (with the reason documented at the opt-out) — so the matrix stays green without weakening the
scenario for everyone else.

---

## Running it

Every test using `@IndexScenarios` is tagged `Tags.IndexScenarios`, so the whole matrix — across all
modules — runs with:

```sh
./gradlew indexScenarioTest
```

A test that builds its own parameters (for example a cartesian product of scenarios and index types
via `@MethodSource`) instead of using `@IndexScenarios` must add `@Tag(Tags.IndexScenarios)` itself.

---

## Extending it

- **New index type** — add an `IndexDefinition` (what to index, how to scan) and a thin test that
  wires it into `@IndexScenarios`. All scenarios then run against it automatically, and it joins the
  `indexScenarioTest` task with no extra wiring.
- **New scenario** — add an `IndexScenario` (`@AutoService`). It immediately applies to every index
  type.

Neither requires touching the shared proto, the other definitions, or the framework core.

---

## Component reference

| Type | Role |
|---|---|
| `IndexScenario` / `@IndexScenarios` / `IndexScenariosArgumentsProvider` | A scenario and its ServiceLoader-based discovery/injection as JUnit arguments. |
| `IndexDefinition` / `IndexDefinitionFactory` | Per-index-type description and its supplier. |
| `IndexTarget` | Roots a definition's value expression at the indexed sub-message; supplies the grouping prefix. |
| `IndexScenarioMetaData` | Builds `RecordMetaData` (primary-key alignment, grouping, joined/unnested wiring). |
| `ScenarioRecords` | Generates records over the shared schema; holds the field/type-name constants. |
| `IndexScenarioModel` | Wraps a definition + context + store builder; store/index operations and result assertions. |
| scenario proto | The shared schema: one record type with a generic indexable sub-message. |

## Future work

- **Index validation** — the scenarios compare what scans return, so they cannot see corruption in
  an index's internal bookkeeping (for example a count) until it changes a scan result. A hook on
  `IndexDefinition` that checks a maintainer's internal invariants would catch that directly.
  `IndexMaintainer` has a `validateEntries` method, but only `ValueIndexMaintainer` implements it;
  scrubbing is another option. Unlike scrubbing, the hook could span several transactions, since the
  data is not changing while it runs.
- **Writes after the build** — every scenario's last mutation is the build itself, so bookkeeping
  that is wrong but not yet harmful never gets the chance to cause a visible failure. A phase that
  keeps writing after the build would expose it.
- **Basic delete** — a scenario that deletes individual records.
- **Combine synthetic types** — run synthetic record types through the other operations,
  particularly deletes.
