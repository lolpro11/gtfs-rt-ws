use std::time::Duration;
use std::{collections::HashMap, convert::Infallible, sync::Arc};
use futures::stream::StreamExt;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;
use warp::filters::ws::WebSocket;
use warp::{ws::Message, Filter, Rejection};
use warp::Reply;
use futures::FutureExt;
use redis_streams::{client_open,StreamCommands};
use redis::from_redis_value;

async fn client_connection(ws: WebSocket, clients: Clients, msg: (String, String)) {
    println!("establishing client connection... {:?}", ws);

    let (client_ws_sender, _client_ws_rcv) = ws.split();
    let (client_sender, client_rcv) = mpsc::unbounded_channel();

    let client_rcv = UnboundedReceiverStream::new(client_rcv);

    tokio::task::spawn(client_rcv.forward(client_ws_sender).map(|result| {
        if let Err(e) = result {
            println!("error sending websocket msg: {}", e);
        }
    }));

    let uuid = Uuid::new_v4().simple().to_string();

    let new_client = Client {
        client_id: uuid.clone(),
        sender: Some(client_sender),
    };

    clients.lock().await.insert(uuid.clone(), new_client);
    //let pubsub = redis_async::client::pubsub::pubsub_connect("127.0.0.1", 6379).await;
    //println!("connected to Redis");
    let pattern = format!("{}-{}", msg.0, msg.1);
    //let mut stream = pubsub.unwrap().psubscribe("*gtfs*").await.unwrap();
    let client = client_open("redis://127.0.0.1:6379/").unwrap();
    let mut con = client.get_connection().unwrap();
    let mut last_id = "0".to_string();
    let mut current_id = "0".to_string();
    let redis_reader_task = tokio::spawn(async move {
        loop {
            let mut x = con.xread(&[pattern.clone()], &[0]);
            if current_id != last_id {
                current_id = last_id;
                x = con.xread(&[pattern.clone()], &[0]);
                let raw_data: Vec<u8> = from_redis_value(x.as_mut().unwrap().keys.iter().last().unwrap().ids.iter().last().unwrap().map.get(format!("gtfsrt|{}|{}", msg.0, msg.1).as_str()).unwrap()).unwrap();
                let locked = &clients.lock().await;
                match locked.get(&uuid) {
                    Some(v) => {
                        if let Some(sender) = &v.sender {
                        let result = sender.send(Ok(Message::binary(raw_data)));
                        match result {
                            Ok(_) => {},
                            Err(_) => {println!("{} disconnected", uuid); break;},
                        };
                    }
                    }
                    None => return,
                }
            }
            last_id = x.unwrap().keys.iter().last().unwrap().ids.iter().last().unwrap().id.clone();
            println!("{:?}", last_id);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    redis_reader_task.await.expect("Redis reader task panicked");
}

async fn ws_handler(
    ws: warp::ws::Ws, args: (String, String), clients: Arc<tokio::sync::Mutex<HashMap<std::string::String, Client>>>) -> Result<impl Reply> {
    println!("ws_handler");

    Ok(ws.on_upgrade(move |socket| client_connection(socket, clients, args)))
}


#[derive(Debug, Clone)]
pub struct Client {
    pub client_id: String,
    pub sender: Option<mpsc::UnboundedSender<std::result::Result<Message, warp::Error>>>,
}

type Clients = Arc<Mutex<HashMap<String, Client>>>;
type Result<T> = std::result::Result<T, Rejection>;

#[tokio::main]
async fn main() {
    let clients: Clients = Arc::new(Mutex::new(HashMap::new()));
    let route = warp::any()
        .and(warp::query::<HashMap<String, String>>())
        .map(|map: HashMap<String, String>| {
            // Extract the value associated with the key "text" if it exists.
            let feed = match map.get("feed") {
                Some(value) => value,
                None => "Key 'feed' not found",
            };
            let category = match map.get("category") {
                Some(value) => value,
                None => "Key 'category' not found",
            };
            println!("{},{}", feed.to_string(), category.to_string());
            (feed.to_string(), category.to_string())
        });


    println!("Configuring WebSocket route");
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(route)
        .and(with_clients(clients.clone()))
        .and_then(ws_handler);

    let routes = ws_route.with(warp::cors().allow_any_origin());

    println!("Starting server");
    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}


fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}
