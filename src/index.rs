use crate::error::{IcebergError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A secondary index that maps extracted field values back to primary keys.
///
/// For example, if your keys are `user:123` with JSON values containing `{"city": "Zurich"}`,
/// you can create a secondary index on "city" to quickly find all users in "Zurich".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecondaryIndex {
    /// Name of this index (e.g., "city_index").
    pub name: String,
    /// The JSON field path this index extracts (e.g., "city" or "address.city").
    pub field_path: String,
    /// Inverted index: field_value → set of primary keys.
    entries: BTreeMap<String, BTreeSet<String>>,
}

impl SecondaryIndex {
    /// Create a new empty secondary index.
    pub fn new(name: String, field_path: String) -> Self {
        Self {
            name,
            field_path,
            entries: BTreeMap::new(),
        }
    }

    /// Index a key-value pair. Extracts the field from the value (assumes JSON).
    /// If the value is not JSON or the field is missing, the key is not indexed.
    pub fn index_entry(&mut self, primary_key: &str, value: &[u8]) {
        // First remove any old entry for this key
        self.remove_key(primary_key);

        // Try to extract the field value
        if let Some(field_val) = self.extract_field(value) {
            self.entries
                .entry(field_val)
                .or_default()
                .insert(primary_key.to_string());
        }
    }

    /// Remove a primary key from the index.
    pub fn remove_key(&mut self, primary_key: &str) {
        let mut empty_values = Vec::new();
        for (val, keys) in self.entries.iter_mut() {
            keys.remove(primary_key);
            if keys.is_empty() {
                empty_values.push(val.clone());
            }
        }
        for val in empty_values {
            self.entries.remove(&val);
        }
    }

    /// Look up primary keys by an exact field value.
    pub fn lookup(&self, field_value: &str) -> Vec<String> {
        self.entries
            .get(field_value)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Range lookup: find keys where the indexed field is in [start, end).
    pub fn range_lookup(&self, start: &str, end: &str) -> Vec<String> {
        use std::ops::Bound;
        let mut result = Vec::new();
        for (_val, keys) in self.entries.range::<String, _>((
            Bound::Included(&start.to_string()),
            Bound::Excluded(&end.to_string()),
        )) {
            result.extend(keys.iter().cloned());
        }
        result.sort();
        result
    }

    /// Prefix lookup on the indexed field values.
    pub fn prefix_lookup(&self, prefix: &str) -> Vec<String> {
        let mut result = Vec::new();
        for (val, keys) in &self.entries {
            if val.starts_with(prefix) {
                result.extend(keys.iter().cloned());
            }
        }
        result.sort();
        result
    }

    /// Get all distinct indexed values.
    pub fn distinct_values(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    /// Number of distinct indexed values.
    pub fn cardinality(&self) -> usize {
        self.entries.len()
    }

    /// Total number of indexed key references.
    pub fn total_entries(&self) -> usize {
        self.entries.values().map(|s| s.len()).sum()
    }

    /// Extract a field value from a JSON byte slice.
    fn extract_field(&self, value: &[u8]) -> Option<String> {
        let parsed: serde_json::Value = serde_json::from_slice(value).ok()?;
        let parts: Vec<&str> = self.field_path.split('.').collect();
        let mut current = &parsed;
        for part in parts {
            current = current.get(part)?;
        }
        match current {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            _ => Some(current.to_string()),
        }
    }
}

/// Manages multiple secondary indexes for a database.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexManager {
    indexes: BTreeMap<String, SecondaryIndex>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new secondary index.
    pub fn create_index(&mut self, name: &str, field_path: &str) -> Result<()> {
        if self.indexes.contains_key(name) {
            return Err(IcebergError::Corruption(format!(
                "index already exists: {}",
                name
            )));
        }
        let idx = SecondaryIndex::new(name.to_string(), field_path.to_string());
        self.indexes.insert(name.to_string(), idx);
        Ok(())
    }

    /// Drop an index.
    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        if self.indexes.remove(name).is_none() {
            return Err(IcebergError::Corruption(format!(
                "index not found: {}",
                name
            )));
        }
        Ok(())
    }

    /// Index a key-value pair across all indexes.
    pub fn on_put(&mut self, key: &str, value: &[u8]) {
        for idx in self.indexes.values_mut() {
            idx.index_entry(key, value);
        }
    }

    /// Remove a key from all indexes.
    pub fn on_delete(&mut self, key: &str) {
        for idx in self.indexes.values_mut() {
            idx.remove_key(key);
        }
    }

    /// Query an index by exact value.
    pub fn query(&self, index_name: &str, value: &str) -> Result<Vec<String>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| IcebergError::Corruption(format!("index not found: {}", index_name)))?;
        Ok(idx.lookup(value))
    }

    /// Query an index by prefix.
    pub fn query_prefix(&self, index_name: &str, prefix: &str) -> Result<Vec<String>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| IcebergError::Corruption(format!("index not found: {}", index_name)))?;
        Ok(idx.prefix_lookup(prefix))
    }

    /// Get an index by name.
    pub fn get_index(&self, name: &str) -> Option<&SecondaryIndex> {
        self.indexes.get(name)
    }

    /// List all index names.
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Rebuild all indexes from a full set of key-value pairs.
    pub fn rebuild_all(&mut self, entries: &[(String, Vec<u8>)]) {
        for idx in self.indexes.values_mut() {
            idx.entries.clear();
            for (key, value) in entries {
                idx.index_entry(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_value(city: &str, age: u32) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "city": city,
            "age": age,
        }))
        .unwrap()
    }

    #[test]
    fn basic_index_lookup() {
        let mut idx = SecondaryIndex::new("city_idx".into(), "city".into());
        idx.index_entry("user:1", &json_value("Zurich", 30));
        idx.index_entry("user:2", &json_value("Berlin", 25));
        idx.index_entry("user:3", &json_value("Zurich", 40));

        let mut result = idx.lookup("Zurich");
        result.sort();
        assert_eq!(result, vec!["user:1", "user:3"]);
        assert_eq!(idx.lookup("Berlin"), vec!["user:2"]);
        assert!(idx.lookup("Paris").is_empty());
    }

    #[test]
    fn index_update_replaces_old_value() {
        let mut idx = SecondaryIndex::new("city_idx".into(), "city".into());
        idx.index_entry("user:1", &json_value("Zurich", 30));
        assert_eq!(idx.lookup("Zurich"), vec!["user:1"]);

        // User moves to Berlin
        idx.index_entry("user:1", &json_value("Berlin", 30));
        assert!(idx.lookup("Zurich").is_empty());
        assert_eq!(idx.lookup("Berlin"), vec!["user:1"]);
    }

    #[test]
    fn remove_key_from_index() {
        let mut idx = SecondaryIndex::new("city_idx".into(), "city".into());
        idx.index_entry("user:1", &json_value("Zurich", 30));
        idx.remove_key("user:1");
        assert!(idx.lookup("Zurich").is_empty());
    }

    #[test]
    fn nested_field_path() {
        let mut idx = SecondaryIndex::new("country_idx".into(), "address.country".into());
        let val = serde_json::to_vec(&serde_json::json!({
            "name": "Alice",
            "address": { "country": "CH", "city": "Zurich" }
        }))
        .unwrap();
        idx.index_entry("user:1", &val);
        assert_eq!(idx.lookup("CH"), vec!["user:1"]);
    }

    #[test]
    fn numeric_field_indexed_as_string() {
        let mut idx = SecondaryIndex::new("age_idx".into(), "age".into());
        idx.index_entry("user:1", &json_value("Zurich", 30));
        assert_eq!(idx.lookup("30"), vec!["user:1"]);
    }

    #[test]
    fn distinct_values() {
        let mut idx = SecondaryIndex::new("city_idx".into(), "city".into());
        idx.index_entry("u:1", &json_value("Zurich", 30));
        idx.index_entry("u:2", &json_value("Berlin", 25));
        idx.index_entry("u:3", &json_value("Zurich", 40));

        let mut vals = idx.distinct_values();
        vals.sort();
        assert_eq!(vals, vec!["Berlin", "Zurich"]);
        assert_eq!(idx.cardinality(), 2);
        assert_eq!(idx.total_entries(), 3);
    }

    #[test]
    fn prefix_lookup() {
        let mut idx = SecondaryIndex::new("city_idx".into(), "city".into());
        idx.index_entry("u:1", &json_value("Zurich", 30));
        idx.index_entry("u:2", &json_value("Zug", 25));
        idx.index_entry("u:3", &json_value("Berlin", 40));

        let result = idx.prefix_lookup("Zu");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn non_json_value_not_indexed() {
        let mut idx = SecondaryIndex::new("city_idx".into(), "city".into());
        idx.index_entry("key:1", b"not json at all");
        assert!(idx.lookup("anything").is_empty());
        assert_eq!(idx.total_entries(), 0);
    }

    #[test]
    fn index_manager_basics() {
        let mut mgr = IndexManager::new();
        mgr.create_index("city", "city").unwrap();
        mgr.create_index("age", "age").unwrap();

        mgr.on_put("u:1", &json_value("Zurich", 30));
        mgr.on_put("u:2", &json_value("Berlin", 25));

        assert_eq!(mgr.query("city", "Zurich").unwrap(), vec!["u:1"]);
        assert_eq!(mgr.query("age", "25").unwrap(), vec!["u:2"]);

        mgr.on_delete("u:1");
        assert!(mgr.query("city", "Zurich").unwrap().is_empty());
    }

    #[test]
    fn index_manager_duplicate_create_fails() {
        let mut mgr = IndexManager::new();
        mgr.create_index("idx", "field").unwrap();
        assert!(mgr.create_index("idx", "field").is_err());
    }

    #[test]
    fn index_manager_drop() {
        let mut mgr = IndexManager::new();
        mgr.create_index("idx", "field").unwrap();
        mgr.drop_index("idx").unwrap();
        assert!(mgr.drop_index("idx").is_err());
    }

    #[test]
    fn index_manager_rebuild() {
        let mut mgr = IndexManager::new();
        mgr.create_index("city", "city").unwrap();

        let entries = vec![
            ("u:1".to_string(), json_value("Zurich", 30)),
            ("u:2".to_string(), json_value("Berlin", 25)),
        ];
        mgr.rebuild_all(&entries);

        assert_eq!(mgr.query("city", "Zurich").unwrap(), vec!["u:1"]);
        assert_eq!(mgr.query("city", "Berlin").unwrap(), vec!["u:2"]);
    }

    #[test]
    fn index_manager_list() {
        let mut mgr = IndexManager::new();
        mgr.create_index("a", "f1").unwrap();
        mgr.create_index("b", "f2").unwrap();
        assert_eq!(mgr.list_indexes(), vec!["a", "b"]);
    }
}
