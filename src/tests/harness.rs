use std::{
    fs, io, mem,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PieceIndex(u64);

impl From<u64> for PieceIndex {
    #[inline]
    fn from(original: u64) -> Self {
        Self(original)
    }
}

impl From<PieceIndex> for u64 {
    #[inline]
    fn from(original: PieceIndex) -> Self {
        original.0
    }
}

impl TryFrom<String> for PieceIndex {
    type Error = <u64 as FromStr>::Err;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse().map(|p| PieceIndex(p))
    }
}

impl PieceIndex {
    /// Size in bytes.
    pub const SIZE: usize = mem::size_of::<u64>();
    /// Piece index 0.
    pub const ZERO: PieceIndex = PieceIndex(0);
    /// Piece index 1.
    pub const ONE: PieceIndex = PieceIndex(1);

    /// Create piece index from bytes.
    #[inline]
    pub const fn from_bytes(bytes: [u8; Self::SIZE]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    /// Convert piece index to bytes.
    #[inline]
    pub const fn to_bytes(self) -> [u8; Self::SIZE] {
        self.0.to_le_bytes()
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct PieceArray([u8; Piece::SIZE]);

impl AsRef<[u8]> for PieceArray {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for PieceArray {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl PieceArray {
    /// Create boxed value without hitting stack overflow
    #[inline]
    pub fn new_boxed() -> Box<Self> {
        // TODO: Should have been just `::new()`, but https://github.com/rust-lang/rust/issues/53827
        // SAFETY: Data structure filled with zeroes is a valid invariant
        unsafe { Box::<Self>::new_zeroed().assume_init() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Piece(pub Vec<u8>);

impl Piece {
    /// Size of a piece (in bytes).
    pub const SIZE: usize = 1048672;
}

// impl Deref for Piece {
//     type Target = PieceArray;

//     #[inline]
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

impl Default for Piece {
    #[inline]
    fn default() -> Self {
        let mut v = Vec::with_capacity(Piece::SIZE);
        unsafe { v.set_len(Piece::SIZE) };
        Self(v)
    }
}

// impl From<Piece> for Vec<u8> {
//     #[inline]
//     fn from(piece: Piece) -> Self {
//         piece.0 .0.to_vec()
//     }
// }

/// Disk piece cache open error
#[derive(Debug, thiserror::Error)]
pub enum DiskPieceCacheError {
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug)]
struct Inner {
    piece_dir: PathBuf,
}

const M: u64 = 1024;

/// Piece cache stored on one disk
#[derive(Debug, Clone)]
pub struct DiskPieceCache {
    inner: Arc<Inner>,
}

impl DiskPieceCache {
    pub fn open(directory: &Path) -> Result<Self, DiskPieceCacheError> {
        Self::open_internal(directory)
    }

    pub(super) fn open_internal(directory: &Path) -> Result<Self, DiskPieceCacheError> {
        Ok(Self {
            inner: Arc::new(Inner {
                piece_dir: directory.to_path_buf(),
            }),
        })
    }

    pub async fn write_piece(
        &self,
        piece_index: PieceIndex,
        piece: Piece,
    ) -> Result<(), DiskPieceCacheError> {
        let (filename, tmp_filename) = self.piece_filenames(piece_index);
        let piece_bytes: Vec<u8> = piece.0;

        if let Some(basedir) = filename.parent() {
            fs::create_dir_all(basedir).map_err(DiskPieceCacheError::Io)?;
        }
        tokio::fs::write(&tmp_filename, piece_bytes)
            .await
            .map_err(DiskPieceCacheError::Io)?;
        tokio::fs::rename(tmp_filename, filename)
            .await
            .map_err(DiskPieceCacheError::Io)?;
        Ok(())
    }

    pub async fn has_piece(&self, piece_index: PieceIndex) -> bool {
        let (filename, _) = self.piece_filenames(piece_index);
        tokio::fs::try_exists(filename).await.unwrap_or(false)
    }

    /// Read piece from cache
    pub async fn read_piece(
        &self,
        piece_index: PieceIndex,
    ) -> Result<Option<Piece>, DiskPieceCacheError> {
        if !self.has_piece(piece_index).await {
            return Ok(None);
        }
        let (filename, _) = self.piece_filenames(piece_index);
        println!("DiskPieceCache read_piece gen filename {filename:?}");

        let bs = fs::read(&filename).map_err(DiskPieceCacheError::Io)?;
        println!("DiskPieceCache read_piece read piece len: {}", bs.len());
        let (piece_bytes, _) = bs.split_at(Piece::SIZE);
        println!("DiskPieceCache read_piece split bs");
        let mut piece = Piece::default();
        println!("DiskPieceCache read_piece copy into piece");
        piece.0.copy_from_slice(piece_bytes);
        Ok(Some(piece))
    }

    fn piece_filenames(&self, piece_index: PieceIndex) -> (PathBuf, PathBuf) {
        let piece_index = u64::from(piece_index);
        let sub_dir = format!("{}", piece_index % M);
        let filename = self
            .inner
            .piece_dir
            .join(&sub_dir)
            .join(u64::from(piece_index).to_string());

        let tmp_filename = self
            .inner
            .piece_dir
            .join(sub_dir)
            .join(format!("{}.tmp", piece_index));
        (filename, tmp_filename)
    }
}

impl crate::DiskCache for DiskPieceCache {
    type Key = PieceIndex;

    type Value = Piece;

    type Error = DiskPieceCacheError;

    fn load(
        &self,
        key: &Self::Key,
    ) -> impl std::future::Future<Output = Result<Option<Self::Value>, Self::Error>> + Send {
        println!("DiskPieceCache wait for load");
        self.read_piece(*key)
    }

    fn store(
        &mut self,
        key: Self::Key,
        value: Self::Value,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        self.write_piece(key, value)
    }

    fn exist(&self, key: &Self::Key) -> impl std::future::Future<Output = bool> + Send {
        self.has_piece(*key)
    }

    fn directory(&self) -> &std::path::Path {
        self.inner.piece_dir.as_path()
    }
}
