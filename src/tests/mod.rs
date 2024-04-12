mod harness;
use harness::{DiskPieceCache, Piece, PieceIndex};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::{sleep, sleep_until, Instant},
};

use anyhow::Result;
use std::{path::Path, sync::Arc, time::Duration};

use crate::{SyncStatus, Syncer};

enum Event {
    Load(PieceIndex),
    Store(PieceIndex, Piece),
    Shutdown(Vec<PieceIndex>),
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

async fn load_and_check_response(index: u64, tx: &UnboundedSender<Request>) {
    let (load_req, res_rx) = new_request(Event::Load(index.into()));
    tx.send(load_req).unwrap();
    if let Response::Load(Ok(SyncStatus::Synced(cache))) = res_rx.await.unwrap() {
        let piece_vec: Vec<_> = cache.0;
        println!("piece {index} synced: {}", piece_vec.len());
        assert_eq!(piece_vec.len(), Piece::SIZE);
    } else {
        panic!();
    }
}

async fn load_and_sync(index: u64, need_sync: bool, tx: &UnboundedSender<Request>) {
    let (load_req, res_rx) = new_request(Event::Load(index.into()));
    tx.send(load_req).unwrap();
    let status = res_rx.await.unwrap();
    match (need_sync, status) {
        (true, Response::Load(Ok(SyncStatus::NeedSync(piece_index)))) => {
            let resp_index: u64 = piece_index.into();
            println!("piece {index} need sync");
            assert_eq!(index, resp_index);
        }
        (false, Response::Load(Ok(SyncStatus::AlreadyInProcess(piece_index)))) => {
            let resp_index: u64 = piece_index.into();
            println!("piece {index} already in-process");
            assert_eq!(index, resp_index);
        }
        _ => panic!(),
    }
}

async fn store_and_check_response(index: u64, piece: Piece, tx: &UnboundedSender<Request>) {
    let (store_req, res_rx) = new_request(Event::Store(index.into(), piece));
    tx.send(store_req).unwrap();
    if let Response::Store(Ok(())) = res_rx.await.unwrap() {
        println!("piece {index} stored");
    } else {
        panic!();
    }
}

async fn run(
    mut rx: UnboundedReceiver<Request>,
    syncer: impl Into<Arc<Syncer<PieceIndex, Piece, DiskPieceCache, 100>>>,
    disk_cache: DiskPieceCache,
) {
    let syncer = syncer.into();
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
                Event::Shutdown(pieces) => {
                    for p in pieces {
                        disk_cache.remove_piece(p).await;
                    }
                    break;
                }
            }
        }
    }
}

#[tokio::test]
async fn integration() {
    let (tx, rx) = unbounded_channel::<Request>();

    let disk_cache = DiskPieceCache::open(&Path::new("./pieces-cache")).unwrap();
    let syncer: Syncer<PieceIndex, Piece, DiskPieceCache, 100> = Syncer::new(
        disk_cache.clone(),
        1_000_000,
        0.01,
        std::time::Duration::from_secs(10),
    );

    // worker 1
    let tx1 = tx.clone();
    tokio::task::spawn(async move {
        println!("worker 1 started");
        // load 67
        load_and_check_response(67, &tx1).await;
        let now = Instant::now();

        // load & store 0
        let piece_0 = Piece::default();
        load_and_sync(0, true, &tx1).await;
        sleep_until(now + Duration::from_secs(5)).await;
        store_and_check_response(0, piece_0, &tx1).await;
        load_and_check_response(0, &tx1).await;
    });

    // worker 2
    let tx2 = tx.clone();
    tokio::task::spawn(async move {
        load_and_check_response(67, &tx2).await;
        // wait for worker1 sync 0 & load
        sleep(Duration::from_secs(3)).await;
        load_and_sync(0, false, &tx2).await;
        sleep(Duration::from_secs(3)).await;
        load_and_check_response(0, &tx2).await;
    });

    // worker 3
    let tx3 = tx;
    tokio::task::spawn(async move {
        load_and_check_response(67, &tx3).await;
        // wait for worker1 sync 0 & load
        sleep(Duration::from_secs(3)).await;
        load_and_sync(0, false, &tx3).await;
        sleep(Duration::from_secs(3)).await;
        load_and_check_response(0, &tx3).await;
        sleep(Duration::from_secs(1)).await;
        let (req, _) = new_request(Event::Shutdown(vec![0.into()]));
        tx3.send(req).unwrap();
    });

    // manager
    run(rx, syncer, disk_cache).await
}

#[tokio::test]
async fn touch_and_wait_in_process_backoff() {
    let (tx, rx) = unbounded_channel::<Request>();
    let disk_cache = DiskPieceCache::open(&Path::new("./pieces-cache")).unwrap();
    let syncer: Arc<Syncer<PieceIndex, Piece, DiskPieceCache, 100>> = Syncer::new(
        disk_cache.clone(),
        1_000_000,
        0.01,
        std::time::Duration::from_secs(1),
    )
    .into();

    let now = Instant::now();

    // worker 1
    let tx1 = tx.clone();
    let syncer2 = syncer.clone();
    tokio::task::spawn(async move {
        // touch piece 0
        syncer2.touch(0u64.into()).await;
        load_and_sync(0, true, &tx1).await;
    });

    // worker 2
    let tx2 = tx.clone();
    tokio::task::spawn(async move {
        sleep_until(now + Duration::from_secs_f32(0.5)).await;
        load_and_sync(0, false, &tx2).await;

        sleep_until(now + Duration::from_secs_f64(1.3)).await;
        load_and_sync(0, true, &tx2).await;

        sleep_until(now + Duration::from_secs_f64(10.5)).await;
        load_and_sync(0, false, &tx2).await;
    });

    // worker 3
    let tx3 = tx;
    tokio::task::spawn(async move {
        sleep_until(now + Duration::from_secs_f32(0.6)).await;
        load_and_sync(0, false, &tx3).await;

        sleep_until(now + Duration::from_secs_f64(1.5)).await;
        load_and_sync(0, false, &tx3).await;

        sleep_until(now + Duration::from_secs_f64(10.3)).await;
        load_and_sync(0, true, &tx3).await;

        sleep(Duration::from_secs(1)).await;
        let (req, _) = new_request(Event::Shutdown(vec![]));
        tx3.send(req).unwrap();
    });

    run(rx, syncer, disk_cache).await
}

#[tokio::test]
async fn touch_many_and_wait_in_process_backoff() {
    let (tx, rx) = unbounded_channel::<Request>();
    let disk_cache = DiskPieceCache::open(&Path::new("./pieces-cache")).unwrap();
    let syncer: Arc<Syncer<PieceIndex, Piece, DiskPieceCache, 100>> = Syncer::new(
        disk_cache.clone(),
        1_000_000,
        0.01,
        std::time::Duration::from_secs(1),
    )
    .into();

    let now = Instant::now();

    // worker 1
    let tx1 = tx.clone();
    let syncer2 = syncer.clone();
    tokio::task::spawn(async move {
        // touch piece 0
        syncer2
            .touch_many(vec![0u64.into(), 1u64.into(), 2u64.into()])
            .await;
        load_and_sync(0, true, &tx1).await;
        load_and_sync(1, true, &tx1).await;
        load_and_sync(2, true, &tx1).await;
    });

    // worker 2
    let tx2 = tx.clone();
    tokio::task::spawn(async move {
        sleep_until(now + Duration::from_secs_f32(0.5)).await;
        load_and_sync(0, false, &tx2).await;
        load_and_sync(1, false, &tx2).await;
        load_and_sync(2, false, &tx2).await;

        sleep_until(now + Duration::from_secs_f64(1.3)).await;
        load_and_sync(0, true, &tx2).await;
        load_and_sync(1, true, &tx2).await;
        load_and_sync(2, true, &tx2).await;

        sleep_until(now + Duration::from_secs_f64(10.5)).await;
        load_and_sync(0, false, &tx2).await;
        load_and_sync(1, false, &tx2).await;
        load_and_sync(2, false, &tx2).await;
    });

    // worker 3
    let tx3 = tx;
    tokio::task::spawn(async move {
        sleep_until(now + Duration::from_secs_f32(0.6)).await;
        load_and_sync(0, false, &tx3).await;
        load_and_sync(1, false, &tx3).await;
        load_and_sync(2, false, &tx3).await;

        sleep_until(now + Duration::from_secs_f64(1.5)).await;
        load_and_sync(0, false, &tx3).await;
        load_and_sync(1, false, &tx3).await;
        load_and_sync(2, false, &tx3).await;

        sleep_until(now + Duration::from_secs_f64(10.3)).await;
        load_and_sync(0, true, &tx3).await;
        load_and_sync(1, true, &tx3).await;
        load_and_sync(2, true, &tx3).await;

        sleep(Duration::from_secs(1)).await;
        let (req, _) = new_request(Event::Shutdown(vec![]));
        tx3.send(req).unwrap();
    });

    run(rx, syncer, disk_cache).await
}

// TODO: add unit test for touch
#[tokio::test]
async fn touch() {
    // TODO
    todo!()
}
