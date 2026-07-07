mod text_metrics;

use crate::handlers::CodeHandlerCatalog;

pub fn register(catalog: &mut CodeHandlerCatalog) {
    catalog.register("example.text_metrics", |registry| {
        registry.register(text_metrics::TextMetricsHandler);
    });
}
