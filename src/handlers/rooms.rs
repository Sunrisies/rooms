use actix_web::{HttpResponse, Result, web};
use sea_orm::{ActiveValue, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, prelude::*};
use serde::{Deserialize, Serialize};

use crate::entities::{messages, room_users, rooms, users};

#[derive(Deserialize)]
pub struct CreateRoomRequest {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub max_users: Option<i32>,
}

#[derive(Serialize)]
pub struct RoomResponse {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub max_users: i32,
    pub user_count: i32,
    // pub created_at: chrono::DateTime<chrono::Utc>,
}

pub async fn create_room(
    db: web::Data<DatabaseConnection>,
    room_data: web::Json<CreateRoomRequest>,
) -> Result<HttpResponse> {
    let room = rooms::ActiveModel {
        id: ActiveValue::Set(room_data.id.clone()),
        name: ActiveValue::Set(room_data.name.clone()),
        description: ActiveValue::Set(room_data.description.clone()),
        max_users: ActiveValue::Set(Some(room_data.max_users.unwrap_or(100))),
        created_at: ActiveValue::Set(None),
        updated_at: ActiveValue::Set(None),
    };

    match rooms::Entity::insert(room).exec(db.get_ref()).await {
        Ok(_) => Ok(HttpResponse::Created().json("Room created successfully")),
        Err(e) => Ok(HttpResponse::BadRequest().json(format!("Error creating room: {}", e))),
    }
}

pub async fn get_room(
    db: web::Data<DatabaseConnection>,
    room_id: web::Path<String>,
) -> Result<HttpResponse> {
    let room = rooms::Entity::find_by_id(room_id.into_inner())
        .one(db.get_ref())
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

    match room {
        Some(room) => {
            let user_count = room_users::Entity::find()
                .filter(room_users::Column::RoomId.eq(&room.id))
                .count(db.get_ref())
                .await
                .unwrap_or(0);

            let response = RoomResponse {
                id: room.id,
                name: room.name,
                description: room.description,
                max_users: 2,
                user_count: user_count as i32,
                // created_at: room.created_at,
            };

            Ok(HttpResponse::Ok().json(response))
        }
        None => Ok(HttpResponse::NotFound().json("Room not found")),
    }
}
