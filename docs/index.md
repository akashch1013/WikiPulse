<div class="wp-hero" markdown>

<span class="wp-eyebrow">WIKIMEDIA DATA · STREAMING + BATCH</span>

# A clearer view of the wiki pulse.

WikiPulse brings live Wikimedia edits and pageview data together to explore how attention and editing activity change over time.

[Explore the architecture](architecture.md){ .wp-button .wp-button-primary }
[Get started on GitHub](https://github.com/akashch1013/WikiPulse#get-started){ .wp-button .wp-button-secondary }

<span class="wp-status">● &nbsp; Under development</span> The build plan tracks what is implemented and what is still ahead.

</div>

## Find your way around

<div class="wp-grid" markdown>

<div class="wp-card" markdown>
<span class="wp-card-label">01 / SYSTEM</span>

### [Architecture →](architecture.md)

Follow the data from Wikimedia sources through streaming, storage, transformations and serving.
</div>

<div class="wp-card" markdown>
<span class="wp-card-label">02 / ANALYTICS</span>

### [Data model →](data-model.md)

Explore the layers, page identity, analytical definitions and evaluation approach.
</div>

<div class="wp-card" markdown>
<span class="wp-card-label">03 / INPUTS</span>

### [Data sources →](sources.md)

Understand source formats, access rules, licences and known data issues.
</div>

<div class="wp-card" markdown>
<span class="wp-card-label">04 / DELIVERY</span>

### [Build plan →](plan.md)

See milestones, workstreams and the criteria used to assess progress.
</div>

</div>

## From signal to insight

<div class="wp-flow" markdown>

**Wikimedia sources** → **Kafka & batch ingestion** → **DuckLake & dbt** → **PostgreSQL** → **API & Grafana**

</div>

The architecture describes the intended end-to-end system, including local and Azure deployments. Individual components and milestones are still being built; consult the [build plan](plan.md) for their scope.

## Reference

- **Decisions:** Browse the architecture decision records in the navigation, starting with the [stream source](decisions/0001-stream-source-page-change.md).
- **Contracts:** Read the [OpenAPI specification](contracts/openapi.yaml), [event schemas](https://github.com/akashch1013/WikiPulse/tree/master/docs/contracts/events), [table contracts](https://github.com/akashch1013/WikiPulse/tree/master/docs/contracts/tables), and [table contract format](contracts/table-contract.schema.json).
- **Local development:** Find setup and commands in the [repository README](https://github.com/akashch1013/WikiPulse#get-started).
