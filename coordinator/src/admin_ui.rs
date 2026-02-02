use axum::response::Html;
use std::path::Path;

pub async fn admin_dashboard() -> Html<String> {
    // Try to load from file first (for development), fall back to embedded
    let html = if Path::new("static/admin.html").exists() {
        std::fs::read_to_string("static/admin.html").unwrap_or_else(|_| FALLBACK_HTML.to_string())
    } else if Path::new("coordinator/static/admin.html").exists() {
        std::fs::read_to_string("coordinator/static/admin.html")
            .unwrap_or_else(|_| FALLBACK_HTML.to_string())
    } else {
        FALLBACK_HTML.to_string()
    };
    Html(html)
}

const FALLBACK_HTML: &str = r#"<!DOCTYPE html>
<html><head><title>Pipeline Admin</title></head>
<body><h1>Error: Could not load admin.html</h1><p>Make sure static/admin.html exists.</p></body>
</html>"#;
