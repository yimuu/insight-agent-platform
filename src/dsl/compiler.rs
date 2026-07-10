use handlebars::{no_escape, Handlebars};

use crate::resources::{actions::ActionRegistry, models::ModelRegistry};

pub struct CompileContext<'a> {
    models: &'a ModelRegistry,
    actions: &'a ActionRegistry,
    templates: Handlebars<'static>,
}

impl<'a> CompileContext<'a> {
    pub fn new(models: &'a ModelRegistry, actions: &'a ActionRegistry) -> Self {
        let mut templates = Handlebars::new();
        templates.set_strict_mode(true);
        templates.register_escape_fn(no_escape);
        Self {
            models,
            actions,
            templates,
        }
    }

    pub fn models(&self) -> &ModelRegistry {
        self.models
    }

    pub fn actions(&self) -> &ActionRegistry {
        self.actions
    }

    pub fn templates(&self) -> &Handlebars<'static> {
        &self.templates
    }

    pub fn into_templates(self) -> Handlebars<'static> {
        self.templates
    }
}
