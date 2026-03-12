use axum::{Router, http::StatusCode, routing::get};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::net::TcpListener;

/// Shared application state passed to all route handlers.
#[derive(Clone)]
struct AppState {
    // pool will be used by data endpoints in future PRs
    #[allow(dead_code)]
    pool: PgPool,
}

/// Returns 200 OK.
async fn ping() -> StatusCode {
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .expect("failed to connect to postgres");

    let state = AppState { pool };

    let app = Router::new()
        .route("/ping", get(ping))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
