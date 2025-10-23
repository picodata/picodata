use hash32::{Hasher, Murmur3Hasher};

pub trait ToHashString {
    fn to_hash_string(&self) -> String;
}

impl<T: ToHashString> ToHashString for &T {
    fn to_hash_string(&self) -> String {
        T::to_hash_string(self)
    }
}

#[must_use]
/// A simple function to calculate the bucket id from a string slice.
/// `(MurMur3 hash at str) % bucket_count + 1`
pub fn str_to_bucket_id(s: &str, bucket_count: u64) -> u64 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write(s.as_bytes());
    u64::from(hasher.finish()) % bucket_count + 1
}

#[must_use]
pub fn bucket_id_by_tuple<T>(sharding_val: impl IntoIterator<Item = T>, bucket_count: u64) -> u64
where
    T: ToHashString,
{
    let hash_str = sharding_val
        .into_iter()
        .map(|v| v.to_hash_string())
        .collect::<String>();
    str_to_bucket_id(&hash_str, bucket_count)
}

#[cfg(test)]
mod tests;
