use std::collections::BTreeMap;

use serde::{ser::SerializeMap as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use super::value::{Identifier, ValueExpr};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum ApiVersion {
    #[serde(rename = "insight.agent/v2")]
    V2,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentKind {
    Agent,
}

/// The root authored vNext workflow document.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawWorkflow {
    pub api_version: ApiVersion,
    pub kind: DocumentKind,
    pub metadata: Metadata,
    pub schema_dialect: String,
    #[serde(default, rename = "$defs")]
    pub definitions: BTreeMap<Identifier, Value>,
    #[serde(default)]
    pub prompts: BTreeMap<Identifier, PromptDeclaration>,
    #[serde(default)]
    pub errors: BTreeMap<Identifier, ErrorDeclaration>,
    pub input: InputContract,
    pub output: OutputContract,
    pub workflow: WorkflowBody,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub id: Identifier,
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InputContract {
    pub schema: Value,
}

/// The schema applies to the stable RunOutput `data` field. The platform owns
/// the outer content/format/data envelope.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OutputContract {
    pub data_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptDeclaration {
    Inline(String),
    File(String),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PromptDeclarationWire {
    Inline(InlinePromptWire),
    File(FilePromptWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InlinePromptWire {
    inline: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePromptWire {
    file: String,
}

impl<'de> Deserialize<'de> for PromptDeclaration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match PromptDeclarationWire::deserialize(deserializer)? {
            PromptDeclarationWire::Inline(value) => Self::Inline(value.inline),
            PromptDeclarationWire::File(value) => Self::File(value.file),
        })
    }
}

impl Serialize for PromptDeclaration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Inline(value) => serialize_single_entry(serializer, "inline", value),
            Self::File(value) => serialize_single_entry(serializer, "file", value),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Workflow,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ErrorDeclaration {
    pub category: ErrorCategory,
    pub code: String,
    pub public_message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowBody {
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: RootResult,
}

/// Operation, parallel, and switch are deliberately one internally tagged
/// union. This keeps the authored shape strict without relying on flattening.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Operation {
        id: Identifier,
        uses: String,
        #[serde(default, rename = "with")]
        inputs: BTreeMap<Identifier, ValueExpr>,
        #[serde(default = "empty_object")]
        config: Value,
    },
    Parallel {
        id: Identifier,
        #[serde(default, rename = "with")]
        inputs: BTreeMap<Identifier, ValueExpr>,
        settle: ParallelSettle,
        #[serde(default)]
        max_concurrency: Option<usize>,
        branches: BTreeMap<Identifier, ParallelBranch>,
    },
    Switch {
        id: Identifier,
        #[serde(default, rename = "with")]
        inputs: BTreeMap<Identifier, ValueExpr>,
        output_schema: Value,
        cases: Vec<SwitchCase>,
        default: SwitchDefault,
    },
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParallelSettle {
    /// Every branch must succeed; the first settleable failure propagates.
    All,
    /// Every branch settles into a typed `Result<T, BranchError>` envelope.
    AllSettled,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranch {
    pub output_schema: Value,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: BlockResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Cel(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PredicateWire {
    cel: String,
}

impl<'de> Deserialize<'de> for Predicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        PredicateWire::deserialize(deserializer).map(|value| Self::Cel(value.cel))
    }
}

impl Serialize for Predicate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Cel(value) => serialize_single_entry(serializer, "cel", value),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SwitchCase {
    pub id: Identifier,
    pub when: Predicate,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: BlockResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SwitchDefault {
    pub id: Identifier,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: BlockResult,
}

/// A child block has exactly one normal return or authored workflow raise.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockResult {
    Return(ValueExpr),
    Raise(Identifier),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BlockResultWire {
    Return(BlockReturnWire),
    Raise(BlockRaiseWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockReturnWire {
    r#return: ValueExpr,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockRaiseWire {
    raise: Identifier,
}

impl<'de> Deserialize<'de> for BlockResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match BlockResultWire::deserialize(deserializer)? {
            BlockResultWire::Return(value) => Self::Return(value.r#return),
            BlockResultWire::Raise(value) => Self::Raise(value.raise),
        })
    }
}

impl Serialize for BlockResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Return(value) => serialize_single_entry(serializer, "return", value),
            Self::Raise(value) => serialize_single_entry(serializer, "raise", value),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Text,
    Markdown,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RootReturn {
    #[serde(default)]
    pub content: Option<ValueExpr>,
    #[serde(default)]
    pub format: Option<OutputFormat>,
    pub data: ValueExpr,
}

/// The workflow root ends in one public success return or authored failure.
#[derive(Debug, Clone, PartialEq)]
pub enum RootResult {
    Return(RootReturn),
    Raise(Identifier),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RootResultWire {
    Return(RootReturnWire),
    Raise(RootRaiseWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootReturnWire {
    r#return: RootReturn,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootRaiseWire {
    raise: Identifier,
}

impl<'de> Deserialize<'de> for RootResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match RootResultWire::deserialize(deserializer)? {
            RootResultWire::Return(value) => Self::Return(value.r#return),
            RootResultWire::Raise(value) => Self::Raise(value.raise),
        })
    }
}

impl Serialize for RootResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Return(value) => serialize_single_entry(serializer, "return", value),
            Self::Raise(value) => serialize_single_entry(serializer, "raise", value),
        }
    }
}

fn serialize_single_entry<S, T>(
    serializer: S,
    key: &'static str,
    value: &T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize + ?Sized,
{
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(key, value)?;
    map.end()
}

pub fn parse_workflow(source: &str) -> Result<RawWorkflow, String> {
    yaml_serde::from_str(source).map_err(|error| format!("vNext workflow parse failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::super::value::{Identifier, ValueExpr};
    use super::{parse_workflow, ParallelSettle, RootResult, Step};

    const VALID: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: parallel_researcher
  name: Parallel Researcher
  description: Typed vNext fixture.
schema_dialect: https://json-schema.org/draft/2020-12/schema
$defs:
  Perspective: {type: object}
prompts:
  system:
    inline: You are concise.
errors:
  all_failed:
    category: workflow
    code: WORKFLOW_ALL_FAILED
    public_message: Every branch failed.
input:
  schema:
    type: object
    required: [question]
output:
  data_schema: {type: object}
workflow:
  steps:
    - kind: operation
      id: prepare
      uses: example.template
      with:
        question: {from: input.question}
      config:
        arbitrary_extension_config: true

    - kind: parallel
      id: perspectives
      with:
        question: {from: steps.prepare.output.question}
      settle: all_settled
      max_concurrency: 2
      branches:
        technical:
          output_schema: {type: object}
          steps:
            - kind: operation
              id: analyze
              uses: ai.chat
              with:
                question: {from: scope.question}
              config: {model: general_chat}
          result:
            return: {from: steps.analyze.output.data}
        risk:
          output_schema: {type: object}
          result:
            raise: all_failed

    - kind: switch
      id: selected
      with:
        results: {from: steps.perspectives.output}
      output_schema: {}
      cases:
        - id: available
          when:
            cel: "scope.results.summary.ok > 0"
          result:
            return: {from: scope.results}
      default:
        id: fallback
        result:
          return: {literal: null}
  result:
    return:
      content: {from: steps.selected.output.answer}
      format: markdown
      data:
        object:
          answer: {from: steps.selected.output.answer}
          count: {literal: 2}
"#;

    #[test]
    fn parses_complete_workflow_with_all_step_variants() {
        let workflow = parse_workflow(VALID).unwrap();

        assert_eq!(workflow.metadata.id.as_str(), "parallel_researcher");
        assert_eq!(workflow.workflow.steps.len(), 3);
        assert!(matches!(workflow.workflow.steps[0], Step::Operation { .. }));
        assert!(matches!(
            workflow.workflow.steps[1],
            Step::Parallel {
                settle: ParallelSettle::AllSettled,
                ..
            }
        ));
        assert!(matches!(workflow.workflow.steps[2], Step::Switch { .. }));
        assert!(matches!(workflow.workflow.result, RootResult::Return(_)));

        let Step::Operation { inputs, .. } = &workflow.workflow.steps[0] else {
            unreachable!();
        };
        assert!(matches!(
            inputs[&Identifier::parse("question").unwrap()],
            ValueExpr::From(_)
        ));
    }

    #[test]
    fn parses_both_parallel_settle_policies() {
        assert_eq!(
            yaml_serde::from_str::<ParallelSettle>("all").unwrap(),
            ParallelSettle::All
        );
        assert_eq!(
            yaml_serde::from_str::<ParallelSettle>("all_settled").unwrap(),
            ParallelSettle::AllSettled
        );
    }

    #[test]
    fn rejects_unknown_fields_at_root_step_and_child_boundaries() {
        let root_unknown = VALID.replace("kind: agent", "kind: agent\nunknown_root_contract: true");
        assert!(parse_workflow(&root_unknown).is_err());

        let step_unknown = VALID.replace(
            "uses: example.template",
            "uses: example.template\n      next: forbidden",
        );
        assert!(parse_workflow(&step_unknown).is_err());

        let child_unknown = VALID.replace(
            "output_schema: {type: object}",
            "output_schema: {type: object}\n          next: forbidden",
        );
        assert!(parse_workflow(&child_unknown).is_err());

        let mixed_business_policy = VALID.replace(
            "settle: all_settled",
            "settle: all_settled\n      require: {min_ok: 1}",
        );
        assert!(parse_workflow(&mixed_business_policy).is_err());
    }

    #[test]
    fn rejects_the_complete_legacy_authored_control_flow_grammar() {
        let mut legacy_documents = vec![
            (
                "entry",
                VALID.replacen("workflow:\n", "entry: prepare\nworkflow:\n", 1),
            ),
            (
                "nodes",
                VALID.replacen("workflow:\n", "nodes: {}\nworkflow:\n", 1),
            ),
            (
                "next",
                VALID.replacen(
                    "uses: example.template",
                    "uses: example.template\n      next: selected",
                    1,
                ),
            ),
        ];
        for legacy_type in [
            "core.fork",
            "core.join",
            "core.branch_end",
            "core.condition",
            "core.select",
            "core.end",
        ] {
            legacy_documents.push((
                legacy_type,
                VALID.replacen("uses: example.template", &format!("type: {legacy_type}"), 1),
            ));
        }

        for (legacy_feature, document) in legacy_documents {
            assert!(
                parse_workflow(&document).is_err(),
                "legacy authored feature '{legacy_feature}' must not enter the v2 AST"
            );
        }
    }

    #[test]
    fn requires_an_explicit_schema_dialect() {
        let missing = VALID.replace(
            "schema_dialect: https://json-schema.org/draft/2020-12/schema\n",
            "",
        );
        let error = parse_workflow(&missing).unwrap_err();
        assert!(error.contains("schema_dialect"), "{error}");
    }

    #[test]
    fn rejects_dynamic_paths_through_the_full_document_parser() {
        let dynamic = VALID.replace("{from: input.question}", "{from: 'steps[chosen].output'}");
        let error = parse_workflow(&dynamic).unwrap_err();
        assert!(error.contains("value path"), "{error}");
    }

    #[test]
    fn rejects_implicit_value_and_missing_block_result_shapes() {
        let implicit = VALID.replace(
            "question: {from: input.question}",
            "question: input.question",
        );
        assert!(parse_workflow(&implicit).is_err());

        let missing_result =
            VALID.replace("          result:\n            raise: all_failed\n", "");
        assert!(parse_workflow(&missing_result).is_err());
    }
}
