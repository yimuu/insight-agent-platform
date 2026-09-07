# Ordinary Run evaluation example

These canonical JSON files are reproducible contract fixtures. The IDs and exact deployment digests are explicit test data, not resources installed in your tenant. `report-missing.json` demonstrates honest unstarted trial accounting; it is not a result of a completed evaluation.

Upload your canonical sample/expected JSON as normal Artifacts, publish a Subject Agent, and implement an Evaluator with the generated `evaluator-input.schema.json` input and the manifest's metric output schema. Substitute those exact references and your Selection/Scheduling policies into a copy of `request.json`, then freeze and upload the final manifest and place its exact Artifact reference in the request.

Follow [the CLI evaluation flow](../../../docs/current/agent-authoring.md) to generate the parent Agent, prepare authorized inputs, publish/run, and collect the report. The parent is a standard FullPlan of ChildAgent calls and AllSettled joins. Actual sample bodies enter Run input, and all reads use the existing content gate.

`product_authoring_examples` regenerates these fixtures through the real shared compiler; its native test independently validates the generated schemas against positive and negative documents.
