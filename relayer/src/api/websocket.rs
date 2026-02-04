use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::Response,
    routing::get,
};
use flume::Receiver;
use futures_util::{SinkExt, StreamExt};
use nostr_sdk::Event;
use serde_json;
use std::sync::Arc;
use tracing::{error, info};

use crate::core::subscription::FanoutMessage;

#[derive(Clone)]
pub struct WsState {
    pub event_rx: Arc<Receiver<Event>>,
    pub fanout_rx: Option<Arc<Receiver<FanoutMessage>>>,
}

// use crate::core::relay_pool::RelayPool;

/// WebSocket handler for streaming events to downstream systems
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
) -> Response {
    let rx = state.event_rx.clone();
    ws.on_upgrade(|socket| handle_socket(socket, rx))
}

/// WebSocket handler for fanout payloads to subscribers
async fn fanout_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
) -> Result<Response, StatusCode> {
    let fanout_rx = match state.fanout_rx.clone() {
        Some(rx) => rx,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    Ok(ws.on_upgrade(|socket| handle_fanout_socket(socket, fanout_rx)))
}

/// Handle individual WebSocket connection
async fn handle_socket(socket: WebSocket, event_rx: Arc<Receiver<Event>>) {
    info!("New WebSocket connection established");

    let (mut sender, mut receiver) = socket.split();

    // Spawn task to send events to client
    let send_task = tokio::spawn(async move {
        let event_rx = event_rx.clone();
        while let Ok(event) = event_rx.recv_async().await {
            let json = match serde_json::to_string(&event) {
                Ok(j) => j,
                Err(e) => {
                    error!("Failed to serialize event: {}", e);
                    continue;
                }
            };

            if let Err(e) = sender.send(Message::Text(json.into())).await {
                error!("Failed to send WebSocket message: {}", e);
                break;
            }
        }
    });

    // Spawn task to receive messages from client (for ping/pong, etc.)
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Close(_) => {
                    info!("WebSocket connection closed by client");
                    break;
                }
                Message::Ping(_data) => {
                    // Handle ping (pong will be sent automatically by axum)
                }
                _ => {}
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }

    info!("WebSocket connection closed");
}

/// Handle WebSocket connection for fanout messages
async fn handle_fanout_socket(socket: WebSocket, fanout_rx: Arc<Receiver<FanoutMessage>>) {
    info!("New fanout WebSocket connection established");

    let (mut sender, mut receiver) = socket.split();

    let send_task = tokio::spawn(async move {
        let fanout_rx = fanout_rx.clone();
        while let Ok(msg) = fanout_rx.recv_async().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(e) => {
                    error!("Failed to serialize fanout message: {}", e);
                    continue;
                }
            };

            if let Err(e) = sender.send(Message::Text(json.into())).await {
                error!("Failed to send fanout WebSocket message: {}", e);
                break;
            }
        }
    });

    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Close(_) = msg {
                break;
            }
        }
    });

    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }

    info!("Fanout WebSocket connection closed");
}

/// Create WebSocket router
pub fn create_websocket_router(
    event_rx: Arc<Receiver<Event>>,
    fanout_rx: Option<Arc<Receiver<FanoutMessage>>>,
) -> Router {
    let state = WsState {
        event_rx,
        fanout_rx,
    };

    Router::new()
        .route("/ws", get(websocket_handler))
        .route("/fanout", get(fanout_handler))
        .with_state(state)
}
