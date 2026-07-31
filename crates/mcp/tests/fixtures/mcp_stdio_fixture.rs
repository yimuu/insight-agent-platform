use std::{
    io::{self, BufRead as _, Write as _},
    thread,
    time::Duration,
};

use serde_json::{json, Value};

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "normal".to_owned());
    if mode == "hang" {
        thread::sleep(Duration::from_secs(60));
        return;
    }
    if mode == "malformed" {
        println!("not-json");
        return;
    }
    if mode == "stderr" {
        eprintln!("fixture-private-stderr");
    }
    let mut request_count = 0_u64;
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        if request.get("id").is_none() {
            if mode == "legacy" && request["method"] == "notifications/initialized" {
                println!(
                    "{}",
                    json!({
                        "jsonrpc":"2.0",
                        "method":"notifications/tools/list_changed",
                        "params":{}
                    })
                );
                io::stdout().flush().unwrap();
            }
            continue;
        }
        request_count += 1;
        let id = request["id"].clone();
        let method = request["method"].as_str().unwrap();
        if mode == "legacy" {
            let result = match method {
                "server/discover" => {
                    println!(
                        "{}",
                        json!({
                            "jsonrpc":"2.0",
                            "id":id,
                            "error":{"code":-32601,"message":"Method not found"}
                        })
                    );
                    io::stdout().flush().unwrap();
                    continue;
                }
                "initialize" => json!({
                    "protocolVersion":"2025-11-25",
                    "capabilities":{
                        "tools":{"listChanged":true},
                        "resources":{"subscribe":true,"listChanged":true},
                        "prompts":{"listChanged":true}
                    },
                    "serverInfo":{"name":"legacy-real-process","version":"1.0.0"}
                }),
                "tools/list" => json!({
                    "tools":[{
                        "name":"legacy_echo",
                        "description":"Legacy real-process fixture.",
                        "inputSchema":{"type":"object","additionalProperties":false}
                    }]
                }),
                "resources/subscribe" => json!({}),
                _ => {
                    println!(
                        "{}",
                        json!({
                            "jsonrpc":"2.0",
                            "id":id,
                            "error":{"code":-32601,"message":"Method not found"}
                        })
                    );
                    io::stdout().flush().unwrap();
                    continue;
                }
            };
            println!("{}", json!({"jsonrpc":"2.0","id":id,"result":result}));
            io::stdout().flush().unwrap();
            continue;
        }
        if method == "subscriptions/listen" {
            println!(
                "{}",
                json!({
                    "jsonrpc":"2.0",
                    "method":"notifications/subscriptions/acknowledged",
                    "params":{
                        "notifications":{"toolsListChanged":true},
                        "_meta":{"io.modelcontextprotocol/subscriptionId":id}
                    }
                })
            );
            println!(
                "{}",
                json!({
                    "jsonrpc":"2.0",
                    "method":"notifications/tools/list_changed",
                    "params":{"_meta":{"io.modelcontextprotocol/subscriptionId":id}}
                })
            );
            io::stdout().flush().unwrap();
            thread::sleep(Duration::from_secs(60));
            return;
        }
        println!(
            "{}",
            json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":{
                    "resultType":"complete",
                    "echoMethod":method,
                    "processId":std::process::id(),
                    "requestCount":request_count
                }
            })
        );
        io::stdout().flush().unwrap();
        if mode == "crash_after_one" {
            return;
        }
    }
}
