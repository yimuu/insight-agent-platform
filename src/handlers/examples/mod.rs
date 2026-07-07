mod text_metrics;

use crate::code::registry::CodeRegistry;

pub fn register(registry: &mut CodeRegistry) {
    registry.register(text_metrics::TextMetricsHandler);
}
