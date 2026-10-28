# WikiPulse

WikiPulse captures live editing activity across Wikimedia projects, joins it with hourly pageview data, and
serves activity, read-surge and edit-burst analytics through a REST API and a Grafana dashboard.

| Document | Content |
|---|---|
| [Architecture](architecture.md) | components, data flow, delivery semantics, measured volumes, local and Azure topology, cost |
| [Data model](data-model.md) | layers, star schema, page identity, analytic definitions and their evaluation |
| [Sources](sources.md) | sources, formats, licences, access rules, known data issues |
| [Build plan](plan.md) | milestones, workstreams, definitions of done |
| [Decisions](decisions/0001-stream-source-page-change.md) | architecture decision records |

## Contracts

- [OpenAPI specification](contracts/openapi.yaml)
- [Event schemas](https://github.com/akashch1013/WikiPulse/tree/master/docs/contracts/events)
- [Table contracts](https://github.com/akashch1013/WikiPulse/tree/master/docs/contracts/tables)
- [Table contract format](contracts/table-contract.schema.json)
