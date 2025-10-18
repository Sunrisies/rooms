use actix_ws::Session;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// 客户端发送的消息格式
#[derive(Serialize, Deserialize, Clone)]
pub struct ClientMessage {
    pub message_type: String,
    pub content: Option<String>,
    pub file_url: Option<String>,
    pub file_name: Option<String>,
    pub file_size: Option<i32>,
    pub retention_hours: Option<i32>,
}

// 服务器广播的消息格式
#[derive(Serialize, Deserialize, Clone)]
pub struct BroadcastMessage {
    pub room_id: String,
    pub user_id: String,
    pub user_nickname: String,
    pub message_type: String,
    pub content: Option<String>,
    pub file_url: Option<String>,
    pub file_name: Option<String>,
    pub file_size: Option<i32>,
    pub retention_hours: i32,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

// 聊天服务器状态
pub struct ChatServer {
    sessions: HashMap<String, HashMap<String, Session>>,
}

impl ChatServer {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    // 发送消息到房间内的所有客户端
    pub async fn send_message(&mut self, room_id: &str, message: &str) {
        if let Some(room_sessions) = self.sessions.get_mut(room_id) {
            for session in room_sessions.values_mut() {
                let _ = session.text(message).await;
            }
        }
    }

    // 获取房间内的连接数量
    pub fn get_room_user_count(&self, room_id: &str) -> usize {
        self.sessions.get(room_id).map_or(0, |s| s.len())
    }

    // 添加连接到房间
    pub fn add_session(&mut self, room_id: String, user_id: String, session: Session) -> String {
        let session_id = Uuid::new_v4().to_string();

        self.sessions
            .entry(room_id.clone())
            .or_insert_with(HashMap::new)
            .insert(session_id.clone(), session);

        session_id
    }

    // 从房间移除连接
    pub fn remove_session(&mut self, room_id: &str, session_id: &str) {
        if let Some(room_sessions) = self.sessions.get_mut(room_id) {
            room_sessions.remove(session_id);

            if room_sessions.is_empty() {
                self.sessions.remove(room_id);
            }
        }
    }

    // 广播系统消息
    pub async fn broadcast_system_message(&mut self, room_id: &str, content: &str) {
        let system_message = BroadcastMessage {
            room_id: room_id.to_string(),
            user_id: "system".to_string(),
            user_nickname: "System".to_string(),
            message_type: "system".to_string(),
            content: Some(content.to_string()),
            file_url: None,
            file_name: None,
            file_size: None,
            retention_hours: 1,
            timestamp: chrono::Utc::now(),
        };

        if let Ok(message_json) = serde_json::to_string(&system_message) {
            self.send_message(room_id, &message_json).await;
        }
    }

    // 广播用户消息
    pub async fn broadcast_user_message(&mut self, message: BroadcastMessage) {
        if let Ok(message_json) = serde_json::to_string(&message) {
            self.send_message(&message.room_id, &message_json).await;
        }
    }
}
