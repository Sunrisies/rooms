mod entities;
mod handlers;
// mod models;
// mod services;
mod websocket;

use actix::prelude::*;
use actix_web::{App, HttpServer, middleware, web};
use sea_orm::{Database, DatabaseConnection};
use std::env;

use handlers::{files, messages, rooms};
use websocket::ChatServer;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let db = Database::connect(&database_url)
        .await
        .expect("Failed to connect to database");

    // 启动聊天服务器
    let chat_server = ChatServer::new().start();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(chat_server.clone()))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(
                actix_cors::Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header()
                    .supports_credentials(),
            )
            .service(
                web::scope("/api")
                    .route("/rooms", web::post().to(rooms::create_room))
                    .route("/rooms/{room_id}", web::get().to(rooms::get_room))
                    .route("/messages", web::post().to(messages::send_message))
                    .route(
                        "/rooms/{room_id}/messages",
                        web::get().to(messages::get_recent_messages),
                    )
                    .route("/upload", web::post().to(files::upload_file)),
            )
            .route(
                "/ws/{room_id}/{user_id}",
                web::get().to(websocket::chat_route),
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
