#!/usr/bin/env python3
"""Render a stable, signature-complete inventory of the root crate's API.

rustdoc JSON IDs, spans, documentation, and compiler-provided blanket/auto
implementations are deliberately unstable.  This program instead follows the
root facade's public reexports, resolves workspace-crate aliases across all of
the supplied rustdoc JSON documents, and emits canonical JSON declarations.

The first input document is the compatibility facade.  Additional documents
are workspace members whose module trees may be reexported by that facade.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union


ItemId = Union[int, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "rustdoc_json",
        type=Path,
        nargs="+",
        help="root facade rustdoc JSON followed by zero or more workspace members",
    )
    parser.add_argument("--rustc-version", required=True)
    parser.add_argument("--workspace-root", type=Path, required=True)
    return parser.parse_args()


def item_kind(value: dict) -> str:
    return next(iter(value["inner"]))


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def sorted_values(values: Iterable[Any]) -> List[Any]:
    return sorted(values, key=stable_json)


@dataclass(frozen=True)
class Node:
    document: int
    item_id: str


@dataclass
class Document:
    path: Path
    payload: dict
    index: Dict[str, dict]
    paths: Dict[str, dict]
    root: Node
    crate_name: str


@dataclass(frozen=True)
class Export:
    path: Tuple[str, ...]
    kind: str
    node: Optional[Node]
    external_target: Optional[str] = None


class Inventory:
    def __init__(self, json_paths: List[Path], workspace_root: Path) -> None:
        self.workspace_root = workspace_root.resolve()
        self.documents: List[Document] = []
        for document_number, json_path in enumerate(json_paths):
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            index = payload["index"]
            root_id = str(payload["root"])
            root_item = index.get(root_id)
            if root_item is None or item_kind(root_item) != "module":
                raise SystemExit(f"rustdoc JSON root item is missing or invalid: {json_path}")
            crate_name = root_item.get("name")
            if not crate_name:
                raise SystemExit(f"rustdoc JSON root item has no crate name: {json_path}")
            self.documents.append(
                Document(
                    path=json_path,
                    payload=payload,
                    index=index,
                    paths=payload.get("paths", {}),
                    root=Node(document_number, root_id),
                    crate_name=crate_name,
                )
            )

        format_versions = {doc.payload["format_version"] for doc in self.documents}
        if len(format_versions) != 1:
            raise SystemExit(
                "workspace rustdoc JSON format versions differ: "
                + ", ".join(map(str, sorted(format_versions)))
            )

        crate_names = [doc.crate_name for doc in self.documents]
        if len(crate_names) != len(set(crate_names)):
            raise SystemExit("duplicate workspace rustdoc crate: " + ", ".join(crate_names))

        # Only a document's own canonical paths participate in cross-document
        # resolution.  Every document also lists paths from its dependencies.
        self.owned_paths: Dict[Tuple[Tuple[str, ...], str], List[Node]] = defaultdict(list)
        self.owned_paths_without_kind: Dict[Tuple[str, ...], List[Node]] = defaultdict(list)
        for document_number, doc in enumerate(self.documents):
            for raw_id, metadata in doc.paths.items():
                path = tuple(metadata.get("path", []))
                kind = metadata.get("kind")
                if not path or path[0] != doc.crate_name or str(raw_id) not in doc.index:
                    continue
                node = Node(document_number, str(raw_id))
                self.owned_paths[(path, str(kind))].append(node)
                self.owned_paths_without_kind[path].append(node)

        self.exports: Set[Export] = set()
        self.exported_paths: Dict[Node, Set[str]] = defaultdict(set)
        self.visited_modules: Set[Tuple[Node, Tuple[str, ...], bool]] = set()
        self.active_modules: Set[Node] = set()
        self.authored_impls_by_target: Dict[Node, Set[Node]] = defaultdict(set)
        self.unbound_authored_impls: Set[Node] = set()

    def document(self, node: Node) -> Document:
        return self.documents[node.document]

    def item(self, node: Optional[Node]) -> Optional[dict]:
        if node is None:
            return None
        return self.document(node).index.get(node.item_id)

    def path_metadata(self, document_number: int, item_id: ItemId) -> dict:
        return self.documents[document_number].paths.get(str(item_id), {})

    def external_node(self, document_number: int, item_id: ItemId) -> Optional[Node]:
        metadata = self.path_metadata(document_number, item_id)
        path = tuple(metadata.get("path", []))
        kind = str(metadata.get("kind"))
        candidates = self.owned_paths.get((path, kind), [])
        if not candidates:
            candidates = self.owned_paths_without_kind.get(path, [])
        if not candidates:
            return None
        if len(candidates) != 1:
            raise SystemExit("ambiguous workspace rustdoc path: " + "::".join(path))
        return candidates[0]

    def resolve(self, document_number: int, item_id: Optional[ItemId]) -> Optional[Node]:
        if item_id is None:
            return None
        raw_id = str(item_id)
        doc = self.documents[document_number]
        metadata = doc.paths.get(raw_id, {})
        canonical = tuple(metadata.get("path", []))

        # An external path can have an index entry for compiler-generated
        # material.  Prefer the defining workspace document whenever loaded.
        if canonical and canonical[0] != doc.crate_name:
            external = self.external_node(document_number, raw_id)
            if external is not None:
                return external
        if raw_id in doc.index:
            return Node(document_number, raw_id)
        return self.external_node(document_number, raw_id)

    def add_export(
        self,
        node: Optional[Node],
        path: Tuple[str, ...],
        kind: Optional[str] = None,
        external_target: Optional[str] = None,
    ) -> None:
        if not path:
            return
        value = self.item(node)
        resolved_kind = kind or (item_kind(value) if value is not None else "unresolved_use")
        export = Export(path, resolved_kind, node, external_target)
        self.exports.add(export)
        if node is not None:
            self.exported_paths[node].add("::".join(path))

    def walk_module(self, node: Node, prefix: Tuple[str, ...], include_module: bool) -> None:
        state = (node, prefix, include_module)
        if state in self.visited_modules:
            return
        self.visited_modules.add(state)
        if node in self.active_modules:
            raise SystemExit(
                "cyclic public module reexport while expanding " + "::".join(prefix)
            )
        module_item = self.item(node)
        if module_item is None or item_kind(module_item) != "module":
            raise SystemExit("public module target is absent from workspace rustdoc JSON")
        if include_module:
            self.add_export(node, prefix, "module")

        self.active_modules.add(node)
        try:
            doc = self.document(node)
            for child_id in module_item["inner"]["module"]["items"]:
                child = doc.index.get(str(child_id))
                if child is None or child.get("visibility") != "public":
                    continue
                child_kind = item_kind(child)
                if child_kind == "use":
                    use = child["inner"]["use"]
                    target_id = use.get("id")
                    target_node = self.resolve(node.document, target_id)
                    target = self.item(target_node)
                    target_metadata = doc.paths.get(str(target_id), {})
                    target_kind = (
                        item_kind(target)
                        if target is not None
                        else target_metadata.get("kind")
                    )
                    target_path = "::".join(target_metadata.get("path", [])) or None
                    if use.get("is_glob"):
                        if target_node is None or target_kind != "module":
                            raise SystemExit(
                                "cannot expand public glob reexport "
                                + (target_path or use.get("source", "<unknown>"))
                                + "; supply that workspace crate's rustdoc JSON"
                            )
                        self.walk_module(target_node, prefix, False)
                        continue
                    alias = use.get("name")
                    if not alias:
                        continue
                    alias_path = prefix + (alias,)
                    if target_kind == "module":
                        if target_node is None:
                            raise SystemExit(
                                "cannot expand public module reexport "
                                + (target_path or use.get("source", alias))
                                + "; supply that workspace crate's rustdoc JSON"
                            )
                        self.walk_module(target_node, alias_path, True)
                    else:
                        self.add_export(
                            target_node,
                            alias_path,
                            str(target_kind or "unresolved_use"),
                            target_path,
                        )
                    continue

                name = child.get("name")
                if not name:
                    continue
                child_path = prefix + (name,)
                child_node = Node(node.document, str(child_id))
                if child_kind == "module":
                    self.walk_module(child_node, child_path, True)
                else:
                    self.add_export(child_node, child_path, child_kind)
        finally:
            self.active_modules.remove(node)

    def discover(self) -> None:
        root_doc = self.documents[0]
        self.walk_module(root_doc.root, (root_doc.crate_name,), True)
        self.discover_authored_impls()

    def impl_target_node(self, impl_node: Node) -> Optional[Node]:
        impl_item = self.item(impl_node)
        if impl_item is None:
            return None
        target = impl_item["inner"]["impl"].get("for", {})
        resolved_path = target.get("resolved_path")
        if resolved_path is None:
            return None
        return self.resolve(impl_node.document, resolved_path.get("id"))

    def discover_authored_impls(self) -> None:
        # An impl may live in a different workspace crate from the type (for
        # example, a durable-owned trait implemented for an engine type).  It
        # therefore cannot be recovered solely from the defining type's
        # `impls` list.  Index every authored workspace impl by its target.
        for document_number, doc in enumerate(self.documents):
            for raw_id, value in doc.index.items():
                if item_kind(value) != "impl":
                    continue
                impl_node = Node(document_number, raw_id)
                if not self.authored_impl(impl_node):
                    continue
                target_node = self.impl_target_node(impl_node)
                if target_node is not None:
                    self.authored_impls_by_target[target_node].add(impl_node)
                else:
                    self.unbound_authored_impls.add(impl_node)

    def facade_path(self, node: Node) -> Optional[str]:
        paths = self.exported_paths.get(node)
        if not paths:
            return None
        return min(paths, key=lambda path: (path.count("::"), path))

    def canonical_node_path(self, node: Node) -> Optional[str]:
        facade = self.facade_path(node)
        if facade is not None:
            return facade
        metadata = self.document(node).paths.get(node.item_id, {})
        path = metadata.get("path", [])
        return "::".join(path) if path else None

    def canonical_reference(
        self, document_number: int, item_id: Optional[ItemId], fallback: str
    ) -> str:
        if item_id is not None:
            node = self.resolve(document_number, item_id)
            if node is not None:
                canonical = self.canonical_node_path(node)
                if canonical:
                    return canonical
            metadata = self.path_metadata(document_number, item_id)
            path = metadata.get("path", [])
            if path:
                return "::".join(path)
        return fallback

    def normalize_path(self, value: dict, document_number: int) -> dict:
        return {
            "path": self.canonical_reference(
                document_number, value.get("id"), value.get("path", "<unknown>")
            ),
            "args": self.normalize_generic_args(value.get("args"), document_number),
        }

    def normalize_generic_args(self, value: Any, document_number: int) -> Any:
        if value is None:
            return None
        if "angle_bracketed" in value:
            data = value["angle_bracketed"]
            return {
                "angle_bracketed": {
                    "args": [
                        self.normalize_generic_arg(arg, document_number)
                        for arg in data.get("args", [])
                    ],
                    "constraints": sorted_values(
                        self.normalize_constraint(constraint, document_number)
                        for constraint in data.get("constraints", [])
                    ),
                }
            }
        if "parenthesized" in value:
            data = value["parenthesized"]
            return {
                "parenthesized": {
                    "inputs": [
                        self.normalize_type(input_type, document_number)
                        for input_type in data.get("inputs", [])
                    ],
                    "output": self.normalize_optional_type(
                        data.get("output"), document_number
                    ),
                }
            }
        return self.normalize_unknown(value, document_number)

    def normalize_generic_arg(self, value: dict, document_number: int) -> dict:
        if "type" in value:
            return {"type": self.normalize_type(value["type"], document_number)}
        if "lifetime" in value:
            return {"lifetime": value["lifetime"]}
        if "const" in value:
            return {"const": self.normalize_constant(value["const"], document_number)}
        if "infer" in value:
            return {"infer": None}
        return self.normalize_unknown(value, document_number)

    def normalize_constraint(self, value: dict, document_number: int) -> dict:
        binding = value.get("binding", {})
        if "equality" in binding:
            normalized_binding = {
                "equality": self.normalize_term(binding["equality"], document_number)
            }
        elif "constraint" in binding:
            normalized_binding = {
                "constraint": sorted_values(
                    self.normalize_bound(bound, document_number)
                    for bound in binding["constraint"]
                )
            }
        else:
            normalized_binding = self.normalize_unknown(binding, document_number)
        return {
            "name": value.get("name"),
            "args": self.normalize_generic_args(value.get("args"), document_number),
            "binding": normalized_binding,
        }

    def normalize_optional_type(self, value: Any, document_number: int) -> Any:
        return None if value is None else self.normalize_type(value, document_number)

    def normalize_type(self, value: dict, document_number: int) -> dict:
        if "resolved_path" in value:
            return {
                "resolved_path": self.normalize_path(
                    value["resolved_path"], document_number
                )
            }
        if "dyn_trait" in value:
            data = value["dyn_trait"]
            traits = []
            for trait in data.get("traits", []):
                traits.append(
                    {
                        "trait": self.normalize_path(trait["trait"], document_number),
                        "generic_params": self.normalize_generic_params(
                            trait.get("generic_params", []), document_number
                        ),
                    }
                )
            return {
                "dyn_trait": {
                    "traits": sorted_values(traits),
                    "lifetime": data.get("lifetime"),
                }
            }
        if "generic" in value:
            return {"generic": value["generic"]}
        if "primitive" in value:
            return {"primitive": value["primitive"]}
        if "function_pointer" in value:
            data = value["function_pointer"]
            return {
                "function_pointer": {
                    "sig": self.normalize_function_sig(data["sig"], document_number),
                    "generic_params": self.normalize_generic_params(
                        data.get("generic_params", []), document_number
                    ),
                    "header": self.normalize_unknown(data.get("header", {}), document_number),
                }
            }
        if "tuple" in value:
            return {
                "tuple": [
                    self.normalize_type(element, document_number)
                    for element in value["tuple"]
                ]
            }
        if "slice" in value:
            return {"slice": self.normalize_type(value["slice"], document_number)}
        if "array" in value:
            data = value["array"]
            return {
                "array": {
                    "type": self.normalize_type(data["type"], document_number),
                    "len": data["len"],
                }
            }
        if "pat" in value:
            data = value["pat"]
            return {
                "pat": {
                    "type": self.normalize_type(data["type"], document_number),
                    "pattern": data.get("pattern", data.get("__pat_unstable_do_not_use")),
                }
            }
        if "impl_trait" in value:
            return {
                "impl_trait": sorted_values(
                    self.normalize_bound(bound, document_number)
                    for bound in value["impl_trait"]
                )
            }
        if "infer" in value:
            return {"infer": None}
        if "raw_pointer" in value:
            data = value["raw_pointer"]
            return {
                "raw_pointer": {
                    "is_mutable": data["is_mutable"],
                    "type": self.normalize_type(data["type"], document_number),
                }
            }
        if "borrowed_ref" in value:
            data = value["borrowed_ref"]
            return {
                "borrowed_ref": {
                    "lifetime": data.get("lifetime"),
                    "is_mutable": data["is_mutable"],
                    "type": self.normalize_type(data["type"], document_number),
                }
            }
        if "qualified_path" in value:
            data = value["qualified_path"]
            return {
                "qualified_path": {
                    "name": data["name"],
                    "args": self.normalize_generic_args(data.get("args"), document_number),
                    "self_type": self.normalize_type(data["self_type"], document_number),
                    "trait": self.normalize_path(data["trait"], document_number),
                }
            }
        return self.normalize_unknown(value, document_number)

    def normalize_constant(self, value: Any, document_number: int) -> Any:
        if isinstance(value, dict):
            return {
                "expr": value.get("expr"),
                "value": value.get("value"),
                "is_literal": value.get("is_literal"),
            }
        return self.normalize_unknown(value, document_number)

    def normalize_term(self, value: dict, document_number: int) -> dict:
        if "type" in value:
            return {"type": self.normalize_type(value["type"], document_number)}
        if "constant" in value:
            return {"constant": self.normalize_constant(value["constant"], document_number)}
        return self.normalize_unknown(value, document_number)

    def normalize_bound(self, value: dict, document_number: int) -> dict:
        if "trait_bound" in value:
            data = value["trait_bound"]
            return {
                "trait_bound": {
                    "trait": self.normalize_path(data["trait"], document_number),
                    "generic_params": self.normalize_generic_params(
                        data.get("generic_params", []), document_number
                    ),
                    "modifier": data.get("modifier"),
                }
            }
        if "outlives" in value:
            return {"outlives": value["outlives"]}
        if "use" in value:
            return {"use": sorted(value["use"])}
        return self.normalize_unknown(value, document_number)

    def normalize_generic_params(self, values: List[dict], document_number: int) -> List[dict]:
        result = []
        for value in values:
            kind = value.get("kind", {})
            if "lifetime" in kind:
                data = kind["lifetime"]
                normalized_kind = {"lifetime": {"outlives": sorted(data["outlives"])}}
            elif "type" in kind:
                data = kind["type"]
                normalized_kind = {
                    "type": {
                        "bounds": sorted_values(
                            self.normalize_bound(bound, document_number)
                            for bound in data.get("bounds", [])
                        ),
                        "default": self.normalize_optional_type(
                            data.get("default"), document_number
                        ),
                        "is_synthetic": data.get("is_synthetic", False),
                    }
                }
            elif "const" in kind:
                data = kind["const"]
                normalized_kind = {
                    "const": {
                        "type": self.normalize_type(data["type"], document_number),
                        "default": data.get("default"),
                    }
                }
            else:
                normalized_kind = self.normalize_unknown(kind, document_number)
            result.append({"name": value.get("name"), "kind": normalized_kind})
        return result

    def normalize_generics(self, value: dict, document_number: int) -> dict:
        where_predicates = [
            self.normalize_where_predicate(predicate, document_number)
            for predicate in value.get("where_predicates", [])
        ]
        return {
            "params": self.normalize_generic_params(value.get("params", []), document_number),
            "where_predicates": sorted_values(where_predicates),
        }

    def normalize_where_predicate(self, value: dict, document_number: int) -> dict:
        if "bound_predicate" in value:
            data = value["bound_predicate"]
            return {
                "bound_predicate": {
                    "type": self.normalize_type(data["type"], document_number),
                    "bounds": sorted_values(
                        self.normalize_bound(bound, document_number)
                        for bound in data.get("bounds", [])
                    ),
                    "generic_params": self.normalize_generic_params(
                        data.get("generic_params", []), document_number
                    ),
                }
            }
        if "lifetime_predicate" in value:
            data = value["lifetime_predicate"]
            return {
                "lifetime_predicate": {
                    "lifetime": data["lifetime"],
                    "outlives": sorted(data.get("outlives", [])),
                }
            }
        if "eq_predicate" in value:
            data = value["eq_predicate"]
            return {
                "eq_predicate": {
                    "lhs": self.normalize_type(data["lhs"], document_number),
                    "rhs": self.normalize_term(data["rhs"], document_number),
                }
            }
        return self.normalize_unknown(value, document_number)

    def normalize_function_sig(self, value: dict, document_number: int) -> dict:
        return {
            # Parameter names are not part of a Rust function's type identity.
            "inputs": [
                self.normalize_type(input_pair[1], document_number)
                for input_pair in value.get("inputs", [])
            ],
            "output": self.normalize_optional_type(value.get("output"), document_number),
            "is_c_variadic": value.get("is_c_variadic", False),
        }

    def normalize_function(self, value: dict, document_number: int) -> dict:
        return {
            "sig": self.normalize_function_sig(value["sig"], document_number),
            "generics": self.normalize_generics(value["generics"], document_number),
            "header": self.normalize_unknown(value["header"], document_number),
            "has_body": value.get("has_body", False),
        }

    def normalize_field(self, node: Node) -> dict:
        value = self.item(node)
        if value is None or item_kind(value) != "struct_field":
            raise SystemExit("rustdoc field item is missing")
        return {
            "name": value.get("name"),
            "public": value.get("visibility") == "public",
            "type": self.normalize_type(value["inner"]["struct_field"], node.document),
        }

    def normalize_field_id(
        self, document_number: int, field_id: Optional[ItemId], position: int
    ) -> dict:
        if field_id is None:
            return {"position": position, "stripped": True}
        node = Node(document_number, str(field_id))
        if self.item(node) is None:
            return {"position": position, "stripped": True}
        return self.normalize_field(node)

    def normalize_variant(self, node: Node) -> dict:
        value = self.item(node)
        if value is None or item_kind(value) != "variant":
            raise SystemExit("rustdoc variant item is missing")
        data = value["inner"]["variant"]
        variant_kind = data.get("kind")
        if isinstance(variant_kind, dict):
            if "tuple" in variant_kind:
                normalized_kind = {
                    "tuple": [
                        self.normalize_field_id(node.document, field_id, position)
                        for position, field_id in enumerate(variant_kind["tuple"])
                    ]
                }
            elif "struct" in variant_kind:
                struct_data = variant_kind["struct"]
                normalized_kind = {
                    "struct": {
                        "fields": [
                            self.normalize_field_id(node.document, field_id, position)
                            for position, field_id in enumerate(struct_data.get("fields", []))
                        ],
                        "has_stripped_fields": struct_data.get(
                            "has_stripped_fields", False
                        ),
                    }
                }
            else:
                normalized_kind = self.normalize_unknown(variant_kind, node.document)
        else:
            normalized_kind = variant_kind
        return {
            "name": value.get("name"),
            "kind": normalized_kind,
            "discriminant": self.normalize_unknown(data.get("discriminant"), node.document),
        }

    def normalize_item_declaration(self, node: Node) -> dict:
        value = self.item(node)
        if value is None:
            raise SystemExit("rustdoc item is missing")
        kind = item_kind(value)
        data = value["inner"][kind]
        if kind == "module":
            return {}
        if kind == "function":
            return self.normalize_function(data, node.document)
        if kind == "struct":
            struct_kind = data["kind"]
            if isinstance(struct_kind, dict) and "plain" in struct_kind:
                shape = {
                    "plain": {
                        "fields": [
                            self.normalize_field(Node(node.document, str(field_id)))
                            for field_id in struct_kind["plain"].get("fields", [])
                        ],
                        "has_stripped_fields": struct_kind["plain"].get(
                            "has_stripped_fields", False
                        ),
                    }
                }
            elif isinstance(struct_kind, dict) and "tuple" in struct_kind:
                shape = {
                    "tuple": [
                        self.normalize_field_id(node.document, field_id, position)
                        for position, field_id in enumerate(struct_kind["tuple"])
                    ]
                }
            else:
                shape = self.normalize_unknown(struct_kind, node.document)
            return {
                "kind": shape,
                "generics": self.normalize_generics(data["generics"], node.document),
            }
        if kind == "union":
            return {
                "fields": [
                    self.normalize_field(Node(node.document, str(field_id)))
                    for field_id in data.get("fields", [])
                ],
                "generics": self.normalize_generics(data["generics"], node.document),
            }
        if kind == "enum":
            return {
                "generics": self.normalize_generics(data["generics"], node.document),
                "has_stripped_variants": data.get("has_stripped_variants", False),
                "variants": [
                    self.normalize_variant(Node(node.document, str(variant_id)))
                    for variant_id in data.get("variants", [])
                ],
            }
        if kind == "variant":
            return self.normalize_variant(node)
        if kind == "struct_field":
            return self.normalize_field(node)
        if kind == "trait":
            associated = []
            for associated_id in data.get("items", []):
                associated_node = Node(node.document, str(associated_id))
                associated_item = self.item(associated_node)
                if associated_item is None:
                    continue
                associated.append(
                    {
                        "name": associated_item.get("name"),
                        "kind": item_kind(associated_item),
                        "declaration": self.normalize_item_declaration(associated_node),
                    }
                )
            return {
                "is_auto": data.get("is_auto", False),
                "is_unsafe": data.get("is_unsafe", False),
                "is_dyn_compatible": data.get("is_dyn_compatible", False),
                "generics": self.normalize_generics(data["generics"], node.document),
                "bounds": sorted_values(
                    self.normalize_bound(bound, node.document)
                    for bound in data.get("bounds", [])
                ),
                "items": sorted_values(associated),
            }
        if kind == "trait_alias":
            return {
                "generics": self.normalize_generics(data["generics"], node.document),
                "params": sorted_values(
                    self.normalize_bound(bound, node.document)
                    for bound in data.get("params", [])
                ),
            }
        if kind == "type_alias":
            return {
                "type": self.normalize_type(data["type"], node.document),
                "generics": self.normalize_generics(data["generics"], node.document),
            }
        if kind == "assoc_type":
            return {
                "generics": self.normalize_generics(data["generics"], node.document),
                "bounds": sorted_values(
                    self.normalize_bound(bound, node.document)
                    for bound in data.get("bounds", [])
                ),
                "type": self.normalize_optional_type(data.get("type"), node.document),
            }
        if kind == "constant":
            return {
                "type": self.normalize_type(data["type"], node.document),
                "has_value": data.get("const") is not None,
            }
        if kind == "assoc_const":
            return {
                "type": self.normalize_type(data["type"], node.document),
                "has_default": data.get("value") is not None,
            }
        if kind == "static":
            return {
                "type": self.normalize_type(data["type"], node.document),
                "is_mutable": data.get("is_mutable", False),
                "is_unsafe": data.get("is_unsafe", False),
            }
        if kind in {"macro", "proc_attribute", "proc_derive", "extern_type", "primitive"}:
            # Macro token bodies and documentation are intentionally excluded;
            # their public existence is still frozen by the path/kind columns.
            return {}
        return self.normalize_unknown(data, node.document)

    def has_workspace_span(self, value: dict) -> bool:
        span = value.get("span")
        if span is None or not span.get("filename"):
            return False
        filename = Path(span["filename"])
        if not filename.is_absolute():
            return not str(filename).startswith("<")
        try:
            filename.resolve().relative_to(self.workspace_root)
            return True
        except ValueError:
            return False

    def authored_impl(self, impl_node: Node) -> bool:
        impl_item = self.item(impl_node)
        if impl_item is None or item_kind(impl_item) != "impl":
            return False
        data = impl_item["inner"]["impl"]
        return (
            not data.get("is_synthetic")
            # rustdoc materializes an authored blanket impl once for every
            # matching concrete type.  Those copies carry `blanket_impl`; the
            # original generic declaration does not.
            and data.get("blanket_impl") is None
            and self.has_workspace_span(impl_item)
        )

    def trait_impl_is_public(self, impl_node: Node) -> bool:
        impl_item = self.item(impl_node)
        assert impl_item is not None
        trait = impl_item["inner"]["impl"].get("trait")
        if trait is None:
            return True
        trait_name = self.canonical_reference(
            impl_node.document, trait.get("id"), trait.get("path", "<unknown>")
        )
        # rustc emits these implementation details for ordinary derives.  They
        # are neither nameable stable contracts nor authored source impls.
        if trait_name in {
            "core::clone::TrivialClone",
            "core::marker::StructuralPartialEq",
        }:
            return False
        trait_node = self.resolve(impl_node.document, trait.get("id"))
        if trait_node is None:
            return True
        trait_item = self.item(trait_node)
        if trait_item is not None and self.has_workspace_span(trait_item):
            return bool(self.exported_paths.get(trait_node))
        trait_doc = self.document(trait_node)
        trait_path = trait_doc.paths.get(trait_node.item_id, {}).get("path", [])
        if trait_path and trait_path[0] in {doc.crate_name for doc in self.documents}:
            return bool(self.exported_paths.get(trait_node))
        return True

    def normalize_impl(self, impl_node: Node) -> dict:
        impl_item = self.item(impl_node)
        if impl_item is None:
            raise SystemExit("rustdoc impl item is missing")
        data = impl_item["inner"]["impl"]
        associated = []
        for associated_id in data.get("items", []):
            associated_node = Node(impl_node.document, str(associated_id))
            associated_item = self.item(associated_node)
            if associated_item is None:
                continue
            associated.append(
                {
                    "name": associated_item.get("name"),
                    "kind": item_kind(associated_item),
                    "declaration": self.normalize_item_declaration(associated_node),
                }
            )
        return {
            "is_unsafe": data.get("is_unsafe", False),
            "is_negative": data.get("is_negative", False),
            "generics": self.normalize_generics(data["generics"], impl_node.document),
            "trait": (
                None
                if data.get("trait") is None
                else self.normalize_path(data["trait"], impl_node.document)
            ),
            "for": self.normalize_type(data["for"], impl_node.document),
            "items": sorted_values(associated),
        }

    def normalize_unknown(self, value: Any, document_number: int) -> Any:
        """Fail-stable normalization for newly added rustdoc schema fields.

        This keeps the gate useful across item kinds absent from today's crate,
        without ever serializing rustdoc's unstable numeric IDs.
        """
        if isinstance(value, dict):
            result = {}
            for key in sorted(value):
                if key == "id":
                    result["id_path"] = self.canonical_reference(
                        document_number, value[key], "<unresolved>"
                    )
                else:
                    result[key] = self.normalize_unknown(value[key], document_number)
            return result
        if isinstance(value, list):
            return [self.normalize_unknown(element, document_number) for element in value]
        return value

    def associated_entries(self, export: Export) -> Iterable[Tuple[str, str, dict]]:
        if export.node is None:
            return
        value = self.item(export.node)
        if value is None:
            return
        kind = item_kind(value)
        data = value["inner"][kind]
        rendered_path = "::".join(export.path)

        if kind == "trait":
            for associated_id in data.get("items", []):
                associated_node = Node(export.node.document, str(associated_id))
                associated_item = self.item(associated_node)
                if associated_item is None or not associated_item.get("name"):
                    continue
                yield (
                    f"{rendered_path}::{associated_item['name']}",
                    f"trait_{item_kind(associated_item)}",
                    self.normalize_item_declaration(associated_node),
                )

        if kind == "enum":
            for variant_id in data.get("variants", []):
                variant_node = Node(export.node.document, str(variant_id))
                variant_item = self.item(variant_node)
                if variant_item is None or not variant_item.get("name"):
                    continue
                variant_path = f"{rendered_path}::{variant_item['name']}"
                yield (variant_path, "variant", self.normalize_variant(variant_node))
                variant_kind = variant_item["inner"]["variant"].get("kind")
                if isinstance(variant_kind, dict):
                    struct_data = variant_kind.get("struct", {})
                    field_ids = variant_kind.get("tuple", struct_data.get("fields", []))
                    for position, field_id in enumerate(field_ids):
                        if field_id is None:
                            continue
                        field_node = Node(export.node.document, str(field_id))
                        field_item = self.item(field_node)
                        if field_item is None:
                            continue
                        field_name = field_item.get("name") or str(position)
                        yield (
                            f"{variant_path}::{field_name}",
                            "field",
                            self.normalize_field(field_node),
                        )

        field_ids: List[ItemId] = []
        if kind == "struct":
            struct_kind = data.get("kind")
            if isinstance(struct_kind, dict):
                field_ids = list(struct_kind.get("plain", {}).get("fields", []))
                field_ids += list(struct_kind.get("tuple", []))
        elif kind == "union":
            field_ids = list(data.get("fields", []))
        for position, field_id in enumerate(field_ids):
            field_node = Node(export.node.document, str(field_id))
            field_item = self.item(field_node)
            if field_item is None or field_item.get("visibility") != "public":
                continue
            field_name = field_item.get("name") or str(position)
            yield (
                f"{rendered_path}::{field_name}",
                "field",
                self.normalize_field(field_node),
            )

        for impl_node in self.authored_impls_by_target.get(export.node, set()):
            if not self.trait_impl_is_public(impl_node):
                continue
            impl_item = self.item(impl_node)
            assert impl_item is not None
            impl_data = impl_item["inner"]["impl"]
            trait = impl_data.get("trait")
            if trait is not None:
                trait_name = self.canonical_reference(
                    impl_node.document, trait.get("id"), trait.get("path", "<unknown>")
                )
                yield (
                    f"impl {trait_name} for {rendered_path}",
                    "trait_impl",
                    self.normalize_impl(impl_node),
                )
                continue
            for associated_id in impl_data.get("items", []):
                associated_node = Node(export.node.document, str(associated_id))
                associated_item = self.item(associated_node)
                if (
                    associated_item is None
                    or associated_item.get("visibility") != "public"
                    or not associated_item.get("name")
                ):
                    continue
                yield (
                    f"{rendered_path}::{associated_item['name']}",
                    f"inherent_{item_kind(associated_item)}",
                    self.normalize_item_declaration(associated_node),
                )

    def lines(self) -> List[Tuple[str, str, str]]:
        entries: Set[Tuple[str, str, str]] = set()
        for export in self.exports:
            path = "::".join(export.path)
            if export.node is None:
                declaration = {"external_target": export.external_target}
            else:
                declaration = self.normalize_item_declaration(export.node)
            entries.add((path, export.kind, stable_json(declaration)))
            for associated_path, associated_kind, associated_declaration in self.associated_entries(
                export
            ):
                entries.add(
                    (
                        associated_path,
                        associated_kind,
                        stable_json(associated_declaration),
                    )
                )

        # Generic/non-nominal blanket impls have no single exported target to
        # attach to, but are themselves downstream-visible API contracts.
        for impl_node in self.unbound_authored_impls:
            if not self.trait_impl_is_public(impl_node):
                continue
            impl_item = self.item(impl_node)
            assert impl_item is not None
            data = impl_item["inner"]["impl"]
            trait = data.get("trait")
            if trait is None:
                continue
            trait_name = self.canonical_reference(
                impl_node.document, trait.get("id"), trait.get("path", "<unknown>")
            )
            target_kind = next(iter(data.get("for", {"unknown": None})))
            entries.add(
                (
                    f"impl {trait_name} for <{target_kind}>",
                    "trait_impl",
                    stable_json(self.normalize_impl(impl_node)),
                )
            )
        return sorted(entries)


def main() -> int:
    args = parse_args()
    inventory = Inventory(args.rustdoc_json, args.workspace_root)
    inventory.discover()

    root_document = inventory.documents[0]
    print("# insight-agent-platform root public API inventory")
    print(f"# rustc: {args.rustc_version}")
    print(f"# rustdoc-json-format: {root_document.payload['format_version']}")
    print("# columns: public Rust path<TAB>rustdoc item kind<TAB>canonical declaration JSON")
    for public_path, kind, declaration in inventory.lines():
        print(f"{public_path}\t{kind}\t{declaration}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
