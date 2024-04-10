mod harness;
use harness::{DiskPieceCache, Piece, PieceIndex};
use tokio::{
    sync::{mpsc::unbounded_channel, oneshot},
    time::sleep,
};

use anyhow::Result;
use std::{path::Path, time::Duration};

use crate::{SyncStatus, Syncer};

enum Event {
    Load(PieceIndex),
    Store(PieceIndex, Piece),
}

enum Response {
    Load(Result<SyncStatus<PieceIndex, Piece>>),
    Store(Result<()>),
}

impl From<Result<SyncStatus<PieceIndex, Piece>>> for Response {
    fn from(value: Result<SyncStatus<PieceIndex, Piece>>) -> Self {
        Response::Load(value)
    }
}

impl From<Result<()>> for Response {
    fn from(value: Result<()>) -> Self {
        Response::Store(value)
    }
}

struct Request {
    event: Event,
    response: oneshot::Sender<Response>,
}

fn new_request(event: Event) -> (Request, oneshot::Receiver<Response>) {
    let (tx, rx) = oneshot::channel();
    (
        Request {
            event,
            response: tx,
        },
        rx,
    )
}

#[tokio::test]
async fn integration() {
    let (tx, mut rx) = unbounded_channel::<Request>();

    let disk_cache = DiskPieceCache::open(&Path::new("./pieces-cache")).unwrap();
    let syncer: Syncer<PieceIndex, Piece, DiskPieceCache, 100> = Syncer::new(
        disk_cache,
        1_000_000,
        0.01,
        std::time::Duration::from_secs(10),
    );
    println!("generated syncer");

    // worker 1
    let tx1 = tx.clone();
    tokio::task::spawn(async move {
        println!("worker 1 started");
        let (load_req, res_rx) = new_request(Event::Load(67.into()));
        tx1.send(load_req).unwrap();
        // let res = ;
        if let Response::Load(Ok(SyncStatus::Synced(cache))) = res_rx.await.unwrap() {
            let piece_vec: Vec<_> = cache.0;
            println!("piece 67 synced: {}", piece_vec.len())
        }
    });

    // worker 2
    let tx2 = tx.clone();
    tokio::task::spawn(async move {
        sleep(Duration::from_secs(1)).await;
    });

    // worker 3
    let tx3 = tx;
    tokio::task::spawn(async move {
        sleep(Duration::from_secs(2)).await;
    });

    // manager
    loop {
        if let Some(Request { event, response }) = rx.recv().await {
            match event {
                Event::Load(piece_index) => {
                    let index: u64 = piece_index.into();
                    println!("handle load request: {index}");
                    let res = syncer.load(piece_index).await.into();
                    println!("load request done, send resp");
                    let _ = response.send(res);
                }
                Event::Store(piece_index, piece) => {
                    let index: u64 = piece_index.into();
                    println!("handle store request: {index}");
                    let _ = response.send(syncer.store(piece_index, piece).await.into());
                }
            }
        }
    }
}
