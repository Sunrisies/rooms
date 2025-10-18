use actix_web::{HttpResponse, Result, web};
use chrono::{Duration, Utc};
use sea_orm::{
    ActiveValue, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, prelude::*,
};
use serde::{Deserialize, Serialize};

use crate::entities::{messages, rooms};
// use crate::websocket::{ChatMessage, ChatServer};

#[derive(Deserialize)]
pub struct SendMessageRequest {
    pub room_id: String,
    pub user_id: i32,
    pub message_type: String,
    pub content: Option<String>,
    pub file_url: Option<String>,
    pub file_name: Option<String>,
    pub file_size: Option<i32>,
    pub retention_hours: Option<i32>,
}

#[derive(Serialize)]
pub struct MessageResponse {
    pub id: i32,
    // pub room_id: String,
    // pub user_id: i32,
    pub message_type: String,
    pub content: Option<String>,
    pub file_url: Option<String>,
    pub file_name: Option<String>,
    pub file_size: Option<i32>,
    pub retention_hours: i32,
    // pub created_at: chrono::DateTime<chrono::Utc>,
}

pub async fn send_message(
    db: web::Data<DatabaseConnection>,
    // chat_server: web::Data<ChatServer>,
    message_data: web::Json<SendMessageRequest>,
) -> Result<HttpResponse> {
    // 验证房间存在
    let room_exists = rooms::Entity::find_by_id(&message_data.room_id)
        .count(db.get_ref())
        .await
        .unwrap_or(0)
        > 0;

    if !room_exists {
        return Ok(HttpResponse::NotFound().json("Room not found"));
    }

    let retention_hours = message_data.retention_hours.unwrap_or(24);
    let expires_at = if retention_hours > 0 {
        Some(Utc::now() + Duration::hours(retention_hours as i64))
    } else {
        None
    };

    let message = messages::ActiveModel {
        room_id: ActiveValue::Set(Some(message_data.room_id.clone())),
        user_id: ActiveValue::Set(Some(message_data.user_id)),
        message_type: ActiveValue::Set(message_data.message_type.clone()),
        content: ActiveValue::Set(message_data.content.clone()),
        file_url: ActiveValue::Set(message_data.file_url.clone()),
        file_name: ActiveValue::Set(message_data.file_name.clone()),
        file_size: ActiveValue::Set(message_data.file_size),
        retention_hours: ActiveValue::Set(Some(retention_hours)),
        created_at: ActiveValue::Set(None),
        expires_at: ActiveValue::Set(expires_at.map(|dt| dt.into())),
        ..Default::default()
    };

    let result = messages::Entity::insert(message).exec(db.get_ref()).await;

    match result {
        Ok(insert_result) => {
            // 通过WebSocket广播消息
            // let chat_message = ChatMessage {
            //     room_id: message_data.room_id.clone(),
            //     user_id: message_data.user_id,
            //     message_type: message_data.message_type.clone(),
            //     content: message_data.content.clone(),
            //     file_url: message_data.file_url.clone(),
            //     file_name: message_data.file_name.clone(),
            //     file_size: message_data.file_size,
            //     retention_hours,
            // };

            // chat_server.do_send(chat_message);

            let response = MessageResponse {
                id: insert_result.last_insert_id,
                // room_id: message_data.room_id.clone(),
                // user_id: message_data.user_id,
                message_type: message_data.message_type.clone(),
                content: message_data.content.clone(),
                file_url: message_data.file_url.clone(),
                file_name: message_data.file_name.clone(),
                file_size: message_data.file_size,
                retention_hours,
                // created_at: Utc::now(),
            };

            Ok(HttpResponse::Created().json(response))
        }
        Err(e) => Ok(HttpResponse::BadRequest().json(format!("Error sending message: {}", e))),
    }
}

pub async fn get_recent_messages(
    db: web::Data<DatabaseConnection>,
    path: web::Path<(String,)>,
    query: web::Query<GetMessagesQuery>,
) -> Result<HttpResponse> {
    let room_id = path.0.clone();
    let limit = query.limit.unwrap_or(10);

    let messages = messages::Entity::find()
        .filter(messages::Column::RoomId.eq(room_id))
        .filter(
            messages::Column::ExpiresAt
                .gt(Utc::now())
                .or(messages::Column::ExpiresAt.is_null()),
        )
        .order_by_desc(messages::Column::CreatedAt)
        .limit(limit as u64)
        .all(db.get_ref())
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

    let response_messages: Vec<MessageResponse> = messages
        .into_iter()
        .map(|msg| MessageResponse {
            id: msg.id,
            // room_id: msg.room_id,
            // user_id: msg.user_id,
            message_type: msg.message_type,
            content: msg.content,
            file_url: msg.file_url,
            file_name: msg.file_name,
            file_size: msg.file_size,
            retention_hours: 10,
            // created_at: msg.created_at.into(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(response_messages))
}

#[derive(Deserialize)]
pub struct GetMessagesQuery {
    pub limit: Option<i32>,
}
