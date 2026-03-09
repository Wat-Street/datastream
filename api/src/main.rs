use axum::{Router, http::StatusCode, routing::get};
use tokio::net::TcpListener;

/// Returns 200 OK.
async fn ping() -> StatusCode {
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/ping", get(ping));

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
