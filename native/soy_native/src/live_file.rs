use rocksdb::LiveFile;
use rustler::NifStruct;

#[derive(Debug, NifStruct)]
#[must_use] // Added to test Issue #152
#[module = "Soy.LiveFile"]
pub struct SoyLiveFile {
    pub column_family_name: String,
    pub name: String,
    pub size: usize,
    pub level: i32,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
    pub num_entries: u64,
    pub num_deletions: u64,
}

impl From<LiveFile> for SoyLiveFile {
    fn from(lf: LiveFile) -> Self {
        SoyLiveFile {
            column_family_name: lf.column_family_name,
            name: lf.name,
            size: lf.size,
            level: lf.level,
            start_key: lf.start_key,
            end_key: lf.end_key,
            num_entries: lf.num_entries,
            num_deletions: lf.num_deletions,
        }
    }
}
