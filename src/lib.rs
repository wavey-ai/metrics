use rml_rtmp::sessions::StreamMetadata;
use serde::{ser::Serializer, Serialize};
use srt_sys::CBytePerfMon;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info};

#[derive(Serialize, Clone, Debug)]
pub enum Ingest {
    Srt,
    Rtmp,
}

#[derive(Serialize, Clone, Debug)]
pub struct StreamInfo {
    pub stream_id: u64,
    pub origin: bool,
    pub ingest: Ingest,
    pub rtmp_metadata: Option<StreamMetadata>,
}

pub struct Streams {
    stream_info: Arc<RwLock<BTreeMap<u64, StreamInfo>>>,
    rtmp_metadata: Arc<RwLock<HashMap<String, StreamMetadata>>>,
    srt_stats: Arc<Mutex<BTreeMap<u64, Arc<Mutex<HashMap<String, CBytePerfMon>>>>>>,
}

impl Streams {
    pub fn new() -> Self {
        let stream_info = Arc::new(RwLock::new(BTreeMap::new()));
        let rtmp_metadata = Arc::new(RwLock::new(HashMap::new()));
        let srt_stats = Arc::new(Mutex::new(BTreeMap::new()));
        Streams {
            stream_info,
            rtmp_metadata,
            srt_stats,
        }
    }

    pub async fn set_srt_stat(&self, stream_id: u64, ip: String, data: CBytePerfMon) {
        let mut srt_stats_lock = self.srt_stats.lock().await;

        let ip_map_arc_mutex = srt_stats_lock
            .entry(stream_id)
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone();

        let mut ip_map = ip_map_arc_mutex.lock().await;

        ip_map.insert(ip, data);
    }

    pub async fn get_srt_stats_list(&self) -> Vec<(u64, String, CBytePerfMon)> {
        let mut list = Vec::new();
        let srt_stats_lock = self.srt_stats.lock().await;

        for (stream_id, region_stats_mutex_arc) in srt_stats_lock.iter() {
            let region_stats_lock = region_stats_mutex_arc.lock().await;

            for (region, stats) in region_stats_lock.iter() {
                list.push((*stream_id, region.clone(), stats.clone()));
            }
        }

        list
    }

    pub async fn add_rtmp_metadata(&self, stream_key: String, data: StreamMetadata) {
        let mut lock = self.rtmp_metadata.write().await;
        lock.insert(stream_key.clone(), data);
    }

    pub async fn add_stream_info(&self, stream_id: u64, origin: bool, stream_key: Option<String>) {
        let mut lock = self.stream_info.write().await;
        let mut rtmp_metadata = None;
        let mut ingest = Ingest::Srt;
        if let Some(key) = stream_key {
            let mut lock = self.rtmp_metadata.write().await;
            rtmp_metadata = lock.remove(&key);
            if rtmp_metadata.is_some() {
                ingest = Ingest::Rtmp;
            }
        }

        let stream_info = StreamInfo {
            stream_id,
            ingest,
            origin,
            rtmp_metadata,
        };
        lock.insert(stream_id, stream_info);
    }

    pub async fn del(&self, id: u64) {
        let mut lock = self.stream_info.write().await;
        lock.remove(&id);
        let mut lock = self.srt_stats.lock().await;
        lock.remove(&id);
    }

    pub async fn vals(&self) -> Vec<StreamInfo> {
        let lock = self.stream_info.read().await;
        lock.values().cloned().collect()
    }
}
