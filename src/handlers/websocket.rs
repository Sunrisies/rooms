// use actix_web::{Error, HttpRequest, HttpResponse, rt, web};
// use actix_ws::AggregatedMessage;
// use futures_util::StreamExt as _;
// use uuid::Uuid;
// pub async fn chat_route(
//     req: HttpRequest,
//     stream: web::Payload,
//     path: web::Path<(String, String)>,
// ) -> Result<HttpResponse, Error> {
//     let (room_id, user_id) = path.into_inner();
//     println!(
//         "New WebSocket connection - Room: {}, User: {}",
//         room_id, user_id
//     );
//     let (res, mut session, stream) = actix_ws::handle(&req, stream)?;
//     println!("New chat connection: {}", req.peer_addr().unwrap());

//     // 为每个连接生成唯一session_id
//     let session_id = Uuid::new_v4().to_string();
//     let mut stream = stream
//         .aggregate_continuations()
//         .max_continuation_size(2_usize.pow(20));
//     // 连接到聊天服务器
//     chat_server.do_send(Connect {
//         addr: session.clone().recipient(),
//         room_id: room_id.clone(),
//         user_id: user_id.clone(),
//         session_id: session_id.clone(),
//     });

//     // 处理消息流
//     actix_rt::spawn(handle_ws_messages(
//         session,
//         msg_stream,
//         chat_server.get_ref().clone(),
//         room_id,
//         user_id,
//         session_id,
//     ));

//     Ok(response)
//     // rt::spawn(async move {
//     //     while let Some(msg) = stream.next().await {
//     //         match msg {
//     //             Ok(AggregatedMessage::Text(text)) => {
//     //                 println!("Received text message: {}", text);
//     //                 session.text(text).await.unwrap();
//     //             }

//     //             Ok(AggregatedMessage::Binary(bin)) => {
//     //                 println!("Received binary message: {:x?}", bin);
//     //                 session.binary(bin).await.unwrap();
//     //             }

//     //             Ok(AggregatedMessage::Ping(msg)) => {
//     //                 println!(
//     //                     "Received ping message: {:x?}, responding with pong message",
//     //                     msg
//     //                 );
//     //                 session.pong(&msg).await.unwrap();
//     //             }

//     //             _ => {}
//     //         }
//     //     }
//     // });

//     // Ok(res)
// }

use crate::websocket::{BroadcastMessage, ChatServer, ClientMessage};

use actix_web::{Error, HttpRequest, HttpResponse, web};
use actix_ws::{Message, Session};
use futures_util::StreamExt;
use serde_json;
use std::sync::Mutex;

// WebSocket路由处理函数
pub async fn chat_route(
    req: HttpRequest,
    stream: web::Payload,
    path: web::Path<(String, String)>, // (room_id, user_id)
    chat_server: web::Data<Mutex<ChatServer>>,
) -> Result<HttpResponse, Error> {
    println!(
        "New WebSocket connection - Room: {}, User: {}",
        path.0, path.1
    );

    let (room_id, user_id) = path.into_inner();

    // 建立WebSocket连接
    let (response, session, msg_stream) = actix_ws::handle(&req, stream)?;

    // 添加到聊天服务器
    let session_id = {
        let mut server = chat_server.lock().unwrap();
        server.add_session(room_id.clone(), user_id.clone(), session.clone())
    };

    // 广播用户加入消息
    {
        let mut server = chat_server.lock().unwrap();
        let user_count = server.get_room_user_count(&room_id);
        server
            .broadcast_system_message(
                &room_id,
                &format!("User joined the room. Online users: {}", user_count),
            )
            .await;
    }

    // 处理消息流
    actix_rt::spawn(handle_ws_messages(
        session,
        msg_stream,
        chat_server,
        room_id,
        user_id,
        session_id,
    ));

    Ok(response)
}

// 处理WebSocket消息
async fn handle_ws_messages(
    mut session: Session,
    mut msg_stream: actix_ws::MessageStream,
    chat_server: web::Data<Mutex<ChatServer>>,
    room_id: String,
    user_id: String,
    session_id: String,
) {
    // 从user_id中提取昵称
    let user_nickname = user_id.clone();

    while let Some(Ok(msg)) = msg_stream.next().await {
        match msg {
            Message::Text(text) => {
                println!(
                    "Received text message from user {} in room {}: {}",
                    user_id, room_id, text
                );

                // 解析客户端消息
                match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(client_msg) => {
                        // 创建广播消息
                        let broadcast_msg = BroadcastMessage {
                            room_id: room_id.clone(),
                            user_id: user_id.clone(),
                            user_nickname: user_nickname.clone(),
                            message_type: client_msg.message_type,
                            content: client_msg.content,
                            file_url: client_msg.file_url,
                            file_name: client_msg.file_name,
                            file_size: client_msg.file_size,
                            retention_hours: client_msg.retention_hours.unwrap_or(24),
                            timestamp: chrono::Utc::now(),
                        };

                        // 广播消息到房间内的所有客户端
                        let mut server = chat_server.lock().unwrap();
                        server.broadcast_user_message(broadcast_msg).await;

                        // 这里可以添加数据库保存逻辑
                        // save_message_to_db(&broadcast_msg).await;
                    }
                    Err(e) => {
                        println!("Failed to parse message: {}", e);

                        // 发送错误消息回客户端
                        let error_msg = serde_json::json!({
                            "error": "Invalid message format",
                            "details": e.to_string()
                        });

                        if let Ok(error_text) = serde_json::to_string(&error_msg) {
                            let _ = session.text(error_text).await;
                        }
                    }
                }
            }
            Message::Binary(bin) => {
                println!(
                    "Received binary message from user {} in room {}: {} bytes",
                    user_id,
                    room_id,
                    bin.len()
                );

                // 可以处理二进制消息，如图片/文件
                // 这里简单回显
                let _ = session.binary(bin).await;
            }
            Message::Ping(bytes) => {
                println!("Received ping from user {} in room {}", user_id, room_id);
                let _ = session.pong(&bytes).await;
            }
            Message::Pong(_) => {
                // 忽略pong消息
            }
            Message::Close(reason) => {
                println!(
                    "WebSocket closed by user {} in room {}: {:?}",
                    user_id, room_id, reason
                );
                break;
            }
            Message::Continuation(_) => {
                // 处理continuation帧
                println!(
                    "Received continuation frame from user {} in room {}",
                    user_id, room_id
                );
            }
            Message::Nop => {
                // 无操作
            }
        }
    }

    // 连接断开，从聊天服务器移除
    println!(
        "WebSocket connection closed for user {} in room {}",
        user_id, room_id
    );

    {
        let mut server = chat_server.lock().unwrap();
        server.remove_session(&room_id, &session_id);
        let user_count = server.get_room_user_count(&room_id);
        // 广播用户离开消息
        server
            .broadcast_system_message(
                &room_id,
                &format!("A user left the room. Online users: {}", user_count),
            )
            .await;
    }
}
