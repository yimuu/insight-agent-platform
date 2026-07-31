use jsonschema::{Draft, Validator};
use serde_json::{json, Value};

#[allow(unused_macros)]
#[macro_use]
#[path = "../../../tests/support/workspace_assets.rs"]
mod workspace_assets;

const SNAPSHOT: &str = workspace_asset_str!("schemas/vendor/mcp-2026-07-28.snapshot.json");

fn schema() -> Value {
    serde_json::from_str(SNAPSHOT).expect("vendored MCP schema must be JSON")
}

fn validator(definition: &str) -> Validator {
    let mut document = schema();
    document
        .as_object_mut()
        .expect("schema root")
        .insert("$ref".to_owned(), json!(format!("#/$defs/{definition}")));
    jsonschema::options()
        .with_draft(Draft::Draft202012)
        .build(&document)
        .unwrap_or_else(|error| panic!("compile MCP definition {definition}: {error}"))
}

fn assert_valid(definition: &str, value: Value) {
    let validator = validator(definition);
    if let Err(error) = validator.validate(&value) {
        panic!("{definition} fixture is invalid: {error}; fixture={value}");
    }
}

fn request_meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {
            "elicitation": {"form": {}, "url": {}}
        },
        "io.modelcontextprotocol/clientInfo": {
            "name": "insight-agent-platform",
            "version": "0.1.0"
        }
    })
}

#[test]
fn snapshot_is_the_pinned_modern_schema() {
    let document = schema();
    let definitions = document["$defs"].as_object().unwrap();
    for required in [
        "DiscoverRequest",
        "InputRequiredResult",
        "SubscriptionsListenRequest",
        "CallToolRequest",
        "ListResourcesRequest",
        "ListPromptsRequest",
        "CompleteRequest",
        "UnsupportedProtocolVersionError",
    ] {
        assert!(
            definitions.contains_key(required),
            "snapshot is missing {required}"
        );
    }
}

#[test]
fn every_modern_core_request_matches_the_upstream_union() {
    let meta = request_meta();
    let fixtures = [
        json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":meta}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{"_meta":request_meta()}}),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
            "_meta":request_meta(),"name":"search","arguments":{"q":"rust"}
        }}),
        json!({"jsonrpc":"2.0","id":4,"method":"resources/list","params":{
            "_meta":request_meta()
        }}),
        json!({"jsonrpc":"2.0","id":5,"method":"resources/templates/list","params":{
            "_meta":request_meta()
        }}),
        json!({"jsonrpc":"2.0","id":6,"method":"resources/read","params":{
            "_meta":request_meta(),"uri":"file:///report.txt"
        }}),
        json!({"jsonrpc":"2.0","id":7,"method":"prompts/list","params":{
            "_meta":request_meta()
        }}),
        json!({"jsonrpc":"2.0","id":8,"method":"prompts/get","params":{
            "_meta":request_meta(),"name":"review","arguments":{"tone":"brief"}
        }}),
        json!({"jsonrpc":"2.0","id":9,"method":"completion/complete","params":{
            "_meta":request_meta(),
            "ref":{"type":"ref/prompt","name":"review"},
            "argument":{"name":"tone","value":"b"}
        }}),
        json!({"jsonrpc":"2.0","id":10,"method":"subscriptions/listen","params":{
            "_meta":request_meta(),
            "notifications":{"toolsListChanged":true,"resourceSubscriptions":["file:///report.txt"]}
        }}),
    ];
    let validator = validator("ClientRequest");
    for fixture in fixtures {
        if let Err(error) = validator.validate(&fixture) {
            panic!("ClientRequest fixture is invalid: {error}; fixture={fixture}");
        }
    }
}

#[test]
fn every_core_result_and_content_variant_matches_upstream() {
    let results = [
        json!({
            "resultType":"complete",
            "supportedVersions":["2026-07-28"],
            "capabilities":{"tools":{}},
            "serverInfo":{"name":"fixture","version":"1.0.0"},
            "ttlMs":1000,
            "cacheScope":"private"
        }),
        json!({
            "resultType":"complete","tools":[{
                "name":"search","inputSchema":{"type":"object"}
            }],"ttlMs":1000,"cacheScope":"private"
        }),
        json!({
            "resultType":"complete","content":[{"type":"text","text":"ok"}],
            "structuredContent":{"ok":true},"isError":false
        }),
        json!({
            "resultType":"complete","resources":[{
                "uri":"file:///report.txt","name":"report"
            }],"ttlMs":1000,"cacheScope":"private"
        }),
        json!({
            "resultType":"complete","resourceTemplates":[{
                "uriTemplate":"file:///{name}","name":"files"
            }],"ttlMs":1000,"cacheScope":"private"
        }),
        json!({
            "resultType":"complete",
            "contents":[{"uri":"file:///report.txt","text":"report"}],
            "ttlMs":1000,"cacheScope":"private"
        }),
        json!({
            "resultType":"complete","prompts":[{"name":"review"}],
            "ttlMs":1000,"cacheScope":"private"
        }),
        json!({
            "resultType":"complete",
            "messages":[{"role":"user","content":{"type":"text","text":"Review this."}}]
        }),
        json!({
            "resultType":"complete","completion":{"values":["brief"],"hasMore":false}
        }),
        json!({
            "resultType":"input_required",
            "requestState":"opaque-state",
            "inputRequests":{"approval":{
                "method":"elicitation/create",
                "params":{
                    "mode":"form",
                    "message":"Approve?",
                    "requestedSchema":{"type":"object","properties":{
                        "approved":{"type":"boolean"}
                    }}
                }
            }}
        }),
    ];
    let validator = validator("ServerResult");
    for fixture in results {
        if let Err(error) = validator.validate(&fixture) {
            panic!("ServerResult fixture is invalid: {error}; fixture={fixture}");
        }
    }

    for content in [
        json!({"type":"text","text":"text"}),
        json!({"type":"image","data":"AA==","mimeType":"image/png"}),
        json!({"type":"audio","data":"AA==","mimeType":"audio/wav"}),
        json!({"type":"resource_link","uri":"file:///a","name":"a"}),
        json!({"type":"resource","resource":{"uri":"file:///a","blob":"AA=="}}),
    ] {
        assert_valid("ContentBlock", content);
    }
}

#[test]
fn standard_and_modern_protocol_errors_match_upstream() {
    for error in [
        json!({"code":-32700,"message":"Parse error"}),
        json!({"code":-32600,"message":"Invalid request"}),
        json!({"code":-32601,"message":"Method not found"}),
        json!({"code":-32602,"message":"Invalid params"}),
        json!({"code":-32603,"message":"Internal error"}),
    ] {
        assert_valid("Error", error);
    }
    assert_valid(
        "HeaderMismatchError",
        json!({"jsonrpc":"2.0","id":1,"error":{"code":-32020,"message":"Header mismatch"}}),
    );
    assert_valid(
        "MissingRequiredClientCapabilityError",
        json!({"jsonrpc":"2.0","id":1,"error":{
            "code":-32021,
            "message":"Missing capability",
            "data":{"requiredCapabilities":{"elicitation":{"form":{}}}}
        }}),
    );
    assert_valid(
        "UnsupportedProtocolVersionError",
        json!({"jsonrpc":"2.0","id":1,"error":{
            "code":-32022,
            "message":"Unsupported version",
            "data":{"requested":"2025-11-25","supported":["2026-07-28"]}
        }}),
    );
}
