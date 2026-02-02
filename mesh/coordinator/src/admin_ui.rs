use axum::response::{Html, IntoResponse};
use std::path::Path;

pub async fn dashboard() -> impl IntoResponse {
    // Try to load from file (supports running from repo root or from coordinator-mesh/)
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
<html>
<head>
    <title>Mesh Coordinator Dashboard</title>
    <style>
        body { font-family: sans-serif; padding: 40px; text-align: center; }
        .error { color: #721c24; background: #f8d7da; padding: 20px; border-radius: 8px; max-width: 600px; margin: 0 auto; }
    </style>
</head>
<body>
    <div class="error">
        <h2>Dashboard HTML not found</h2>
        <p>Could not load static/admin.html or coordinator-mesh/static/admin.html</p>
        <p>Make sure you're running from the correct directory.</p>
    </div>
</body>
</html>
"#;
