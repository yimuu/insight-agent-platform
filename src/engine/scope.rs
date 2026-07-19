use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

use super::{DynamicKey, LegId, ModelError, NodeId, ScopeInstanceId};

const SCOPE_INSTANCE_INVALID: &str = "ENGINE_SCOPE_INSTANCE_INVALID";

/// Stable Map identity is independent from persisted declaration order. When
/// no authored key exists, the ordinal is explicitly used as the identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MapItemIdentity {
    BusinessKey(DynamicKey),
    Ordinal(u32),
}

impl MapItemIdentity {
    /// Canonical persisted identity shared by control-model and scheduler repositories.
    pub fn stable_dynamic_key(&self) -> String {
        match self {
            Self::BusinessKey(key) => format!("key:{}", key.as_str()),
            Self::Ordinal(ordinal) => format!("ordinal:{ordinal}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScopeKind {
    Root,
    MapItem {
        owner: NodeId,
        identity: MapItemIdentity,
        ordinal: u32,
    },
    LoopIteration {
        owner: NodeId,
        iteration: u64,
    },
    SubflowInvocation {
        owner: NodeId,
        invocation_key: DynamicKey,
    },
    AgentLoopTurn {
        owner: NodeId,
        turn: u64,
    },
    ParallelLeg {
        owner: NodeId,
        leg_id: LegId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScopeInstance {
    id: ScopeInstanceId,
    parent: Option<ScopeInstanceId>,
    kind: ScopeKind,
}

impl<'de> Deserialize<'de> for ScopeInstance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            id: ScopeInstanceId,
            parent: Option<ScopeInstanceId>,
            kind: ScopeKind,
        }

        let wire = Wire::deserialize(deserializer)?;
        let scope = Self {
            id: wire.id,
            parent: wire.parent,
            kind: wire.kind,
        };
        scope.validate().map_err(D::Error::custom)?;
        Ok(scope)
    }
}

impl ScopeInstance {
    pub fn root() -> Self {
        Self {
            id: ScopeInstanceId::root(),
            parent: None,
            kind: ScopeKind::Root,
        }
    }

    pub fn map_item_with_key(
        parent: &ScopeInstanceId,
        owner: NodeId,
        item_key: DynamicKey,
        ordinal: u32,
    ) -> Result<Self, ModelError> {
        Self::map_item(
            parent,
            owner,
            MapItemIdentity::BusinessKey(item_key),
            ordinal,
        )
    }

    pub fn map_item_by_ordinal(
        parent: &ScopeInstanceId,
        owner: NodeId,
        ordinal: u32,
    ) -> Result<Self, ModelError> {
        Self::map_item(parent, owner, MapItemIdentity::Ordinal(ordinal), ordinal)
    }

    pub fn loop_iteration(
        parent: &ScopeInstanceId,
        owner: NodeId,
        iteration: u64,
    ) -> Result<Self, ModelError> {
        Self::child(parent, ScopeKind::LoopIteration { owner, iteration })
    }

    pub fn subflow(
        parent: &ScopeInstanceId,
        owner: NodeId,
        invocation_key: DynamicKey,
    ) -> Result<Self, ModelError> {
        Self::child(
            parent,
            ScopeKind::SubflowInvocation {
                owner,
                invocation_key,
            },
        )
    }

    pub fn agent_loop_turn(
        parent: &ScopeInstanceId,
        owner: NodeId,
        turn: u64,
    ) -> Result<Self, ModelError> {
        Self::child(parent, ScopeKind::AgentLoopTurn { owner, turn })
    }

    pub fn parallel_leg(
        parent: &ScopeInstanceId,
        owner: NodeId,
        leg_id: LegId,
    ) -> Result<Self, ModelError> {
        Self::child(parent, ScopeKind::ParallelLeg { owner, leg_id })
    }

    pub fn id(&self) -> &ScopeInstanceId {
        &self.id
    }

    pub fn parent(&self) -> Option<&ScopeInstanceId> {
        self.parent.as_ref()
    }

    pub fn kind(&self) -> &ScopeKind {
        &self.kind
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        if let ScopeKind::MapItem {
            identity: MapItemIdentity::Ordinal(identity_ordinal),
            ordinal,
            ..
        } = &self.kind
        {
            if identity_ordinal != ordinal {
                return Err(ModelError::new(
                    SCOPE_INSTANCE_INVALID,
                    "ordinal Map identity must match its persisted declaration order",
                ));
            }
        }
        match (&self.parent, &self.kind) {
            (None, ScopeKind::Root) if self.id == ScopeInstanceId::root() => Ok(()),
            (Some(parent), kind @ ScopeKind::MapItem { .. })
            | (Some(parent), kind @ ScopeKind::LoopIteration { .. })
            | (Some(parent), kind @ ScopeKind::SubflowInvocation { .. })
            | (Some(parent), kind @ ScopeKind::AgentLoopTurn { .. })
            | (Some(parent), kind @ ScopeKind::ParallelLeg { .. }) => {
                let expected = Self::derived_id(parent, kind)?;
                if self.id != expected {
                    return Err(ModelError::new(
                        SCOPE_INSTANCE_INVALID,
                        "scope instance ID does not match its parent and stable identity",
                    ));
                }
                Ok(())
            }
            _ => Err(ModelError::new(
                SCOPE_INSTANCE_INVALID,
                "root and child scope identity hierarchy is inconsistent",
            )),
        }
    }

    fn map_item(
        parent: &ScopeInstanceId,
        owner: NodeId,
        identity: MapItemIdentity,
        ordinal: u32,
    ) -> Result<Self, ModelError> {
        Self::child(
            parent,
            ScopeKind::MapItem {
                owner,
                identity,
                ordinal,
            },
        )
    }

    fn child(parent: &ScopeInstanceId, kind: ScopeKind) -> Result<Self, ModelError> {
        let id = Self::derived_id(parent, &kind)?;
        Ok(Self {
            id,
            parent: Some(parent.clone()),
            kind,
        })
    }

    fn derived_id(
        parent: &ScopeInstanceId,
        kind: &ScopeKind,
    ) -> Result<ScopeInstanceId, ModelError> {
        let (owner, discriminator) = match kind {
            ScopeKind::Root => {
                return Err(ModelError::new(
                    SCOPE_INSTANCE_INVALID,
                    "root scope cannot be derived from a parent",
                ));
            }
            ScopeKind::MapItem {
                owner, identity, ..
            } => (owner, format!("map:{}", identity.stable_dynamic_key())),
            ScopeKind::LoopIteration { owner, iteration } => (owner, format!("loop:{iteration}")),
            ScopeKind::SubflowInvocation {
                owner,
                invocation_key,
            } => (owner, format!("subflow:{}", invocation_key.as_str())),
            ScopeKind::AgentLoopTurn { owner, turn } => (owner, format!("agent_loop:{turn}")),
            ScopeKind::ParallelLeg { owner, leg_id } => {
                (owner, format!("parallel_leg:{}", leg_id.as_str()))
            }
        };
        ScopeInstanceId::derive(parent, owner, &discriminator)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn stable_map_key_identity_does_not_change_when_order_changes() {
        let root = ScopeInstance::root();
        let owner = NodeId::new("items").unwrap();
        let first = ScopeInstance::map_item_with_key(
            root.id(),
            owner.clone(),
            DynamicKey::new("patient-1").unwrap(),
            0,
        )
        .unwrap();
        let reordered = ScopeInstance::map_item_with_key(
            root.id(),
            owner.clone(),
            DynamicKey::new("patient-1").unwrap(),
            9,
        )
        .unwrap();
        let second = ScopeInstance::map_item_with_key(
            root.id(),
            owner,
            DynamicKey::new("patient-2").unwrap(),
            1,
        )
        .unwrap();

        assert_eq!(first.id(), reordered.id());
        assert_ne!(first.id(), second.id());
        assert_eq!(first.parent(), Some(root.id()));
    }

    #[test]
    fn ordinal_map_identity_is_explicit_and_order_sensitive() {
        let root = ScopeInstance::root();
        let owner = NodeId::new("items").unwrap();
        let first = ScopeInstance::map_item_by_ordinal(root.id(), owner.clone(), 0).unwrap();
        let second = ScopeInstance::map_item_by_ordinal(root.id(), owner, 1).unwrap();
        assert_ne!(first.id(), second.id());
    }

    #[test]
    fn map_storage_identity_is_typed_once_and_business_prefixes_do_not_collide() {
        let plain = MapItemIdentity::BusinessKey(DynamicKey::new("x").unwrap());
        let prefixed = MapItemIdentity::BusinessKey(DynamicKey::new("key:x").unwrap());
        let ordinal_looking = MapItemIdentity::BusinessKey(DynamicKey::new("ordinal:0").unwrap());
        let ordinal = MapItemIdentity::Ordinal(0);

        assert_eq!(plain.stable_dynamic_key(), "key:x");
        assert_eq!(prefixed.stable_dynamic_key(), "key:key:x");
        assert_eq!(ordinal_looking.stable_dynamic_key(), "key:ordinal:0");
        assert_eq!(ordinal.stable_dynamic_key(), "ordinal:0");
        assert_ne!(plain, prefixed);
        assert_ne!(
            ordinal_looking.stable_dynamic_key(),
            ordinal.stable_dynamic_key()
        );
    }

    #[test]
    fn parallel_leg_identity_uses_stable_leg_id_not_declaration_order() {
        let root = ScopeInstance::root();
        let owner = NodeId::new("analyses").unwrap();
        let technical =
            ScopeInstance::parallel_leg(root.id(), owner.clone(), LegId::new("technical").unwrap())
                .unwrap();
        let technical_again =
            ScopeInstance::parallel_leg(root.id(), owner.clone(), LegId::new("technical").unwrap())
                .unwrap();
        let risk =
            ScopeInstance::parallel_leg(root.id(), owner, LegId::new("risk").unwrap()).unwrap();

        assert_eq!(technical.id(), technical_again.id());
        assert_ne!(technical.id(), risk.id());
    }

    #[test]
    fn scope_deserialization_recomputes_identity_and_rejects_unknown_fields() {
        let scope = ScopeInstance::loop_iteration(
            ScopeInstance::root().id(),
            NodeId::new("review_loop").unwrap(),
            2,
        )
        .unwrap();
        let encoded = serde_json::to_value(&scope).unwrap();
        assert_eq!(
            serde_json::from_value::<ScopeInstance>(encoded.clone()).unwrap(),
            scope
        );

        let mut wrong_id = encoded.clone();
        wrong_id["id"] = json!(ScopeInstanceId::root().as_str());
        assert!(serde_json::from_value::<ScopeInstance>(wrong_id).is_err());

        let mut unknown = encoded;
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ScopeInstance>(unknown).is_err());
    }
}
