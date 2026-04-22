use smol_str::SmolStr;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CopyFlushThresholds {
    rows: usize,
    bytes: Option<usize>,
}

impl CopyFlushThresholds {
    pub(crate) fn new(rows: usize, bytes: Option<usize>) -> Self {
        Self { rows, bytes }
    }

    pub(crate) fn would_exceed(
        self,
        rows: usize,
        bytes: usize,
        next_row_bytes: usize,
    ) -> Option<CopyFlushReasonKind> {
        if rows == 0 {
            return None;
        }
        if rows.saturating_add(1) > self.rows {
            return Some(CopyFlushReasonKind::Rows);
        }
        if self
            .bytes
            .is_some_and(|max_bytes| bytes.saturating_add(next_row_bytes) > max_bytes)
        {
            return Some(CopyFlushReasonKind::Bytes);
        }
        None
    }

    pub(crate) fn reached(self, rows: usize, bytes: usize) -> Option<CopyFlushReasonKind> {
        if rows >= self.rows {
            return Some(CopyFlushReasonKind::Rows);
        }
        if self.bytes.is_some_and(|max_bytes| bytes >= max_bytes) {
            return Some(CopyFlushReasonKind::Bytes);
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyFlushReasonKind {
    Rows,
    Bytes,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CopyFlushThresholdsByScope {
    pub(crate) session: CopyFlushThresholds,
    pub(crate) destination: CopyFlushThresholds,
}

impl CopyFlushThresholdsByScope {
    pub(crate) fn new(session: CopyFlushThresholds, destination: CopyFlushThresholds) -> Self {
        Self {
            session,
            destination,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ShardedCopyDestination {
    Local,
    Replicaset(SmolStr),
}

#[derive(Debug, Default)]
pub(crate) struct PendingCopyBatch {
    pub(super) encoded_rows: Vec<Vec<u8>>,
    bytes: usize,
}

impl PendingCopyBatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.encoded_rows.is_empty()
    }

    pub(crate) fn rows(&self) -> usize {
        self.encoded_rows.len()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn would_exceed(
        &self,
        next_row_bytes: usize,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        limits.would_exceed(self.rows(), self.bytes, next_row_bytes)
    }

    pub(crate) fn reached(&self, limits: CopyFlushThresholds) -> Option<CopyFlushReasonKind> {
        limits.reached(self.rows(), self.bytes)
    }

    pub(crate) fn push(&mut self, row: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(row.len());
        self.encoded_rows.push(row);
    }

    pub(crate) fn clear(&mut self) {
        self.bytes = 0;
        self.encoded_rows.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_batch_tracks_thresholds() {
        let mut batch = PendingCopyBatch::default();
        let limits = CopyFlushThresholds::new(2, Some(4));

        let first = vec![1, 2];
        assert_eq!(batch.would_exceed(first.len(), limits), None);

        batch.push(first);
        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.bytes(), 2);
        assert_eq!(batch.reached(limits), None);

        let second = vec![3, 4, 5];
        assert_eq!(
            batch.would_exceed(second.len(), limits),
            Some(CopyFlushReasonKind::Bytes)
        );
    }

    #[test]
    fn pending_batch_can_omit_byte_threshold() {
        let mut batch = PendingCopyBatch::default();
        let limits = CopyFlushThresholds::new(3, None);

        batch.push(vec![1, 2, 3]);

        let next = vec![4, 5, 6];
        assert_eq!(batch.would_exceed(next.len(), limits), None);
    }

    #[test]
    fn pending_batch_tracks_bytes() {
        let mut batch = PendingCopyBatch::default();
        batch.push(vec![3, 4, 5]);
        assert_eq!(batch.bytes(), 3);
    }
}
