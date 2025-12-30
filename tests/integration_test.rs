//! Integration test for duckdb_manifold extension
//!
//! This test creates a ManifoldDB database with test data, then uses
//! Python/DuckDB to verify the extension can read it.

use std::collections::HashMap;
use std::process::Command;

use manifoldb_core::types::{Entity, Edge, Value, EntityId, EdgeId, Label, EdgeType};
use manifoldb_core::encoding::Encoder;
use manifoldb_storage::backends::RedbEngine;
use manifoldb_storage::{StorageEngine, Transaction};

fn create_test_database(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let engine = RedbEngine::open(path)?;

    // Create some test entities
    let mut tx = engine.begin_write()?;

    // Entity 1: Person named Alice
    let entity1 = Entity {
        id: EntityId::from(1u64),
        labels: vec![Label::new("Person")],
        properties: {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        },
    };
    let key1 = 1u64.to_be_bytes();
    tx.put("nodes", &key1, &entity1.encode()?)?;

    // Entity 2: Person named Bob
    let entity2 = Entity {
        id: EntityId::from(2u64),
        labels: vec![Label::new("Person")],
        properties: {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        },
    };
    let key2 = 2u64.to_be_bytes();
    tx.put("nodes", &key2, &entity2.encode()?)?;

    // Entity 3: Company
    let entity3 = Entity {
        id: EntityId::from(3u64),
        labels: vec![Label::new("Company")],
        properties: {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
            props.insert("founded".to_string(), Value::Int(1990));
            props
        },
    };
    let key3 = 3u64.to_be_bytes();
    tx.put("nodes", &key3, &entity3.encode()?)?;

    // Edge 1: Alice WORKS_AT Acme Corp
    let edge1 = Edge {
        id: EdgeId::from(100u64),
        source: EntityId::from(1u64),
        target: EntityId::from(3u64),
        edge_type: EdgeType::new("WORKS_AT"),
        properties: {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        },
    };
    let edge_key1 = 100u64.to_be_bytes();
    tx.put("edges", &edge_key1, &edge1.encode()?)?;

    // Edge 2: Bob WORKS_AT Acme Corp
    let edge2 = Edge {
        id: EdgeId::from(101u64),
        source: EntityId::from(2u64),
        target: EntityId::from(3u64),
        edge_type: EdgeType::new("WORKS_AT"),
        properties: {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2022));
            props
        },
    };
    let edge_key2 = 101u64.to_be_bytes();
    tx.put("edges", &edge_key2, &edge2.encode()?)?;

    // Edge 3: Alice KNOWS Bob
    let edge3 = Edge {
        id: EdgeId::from(102u64),
        source: EntityId::from(1u64),
        target: EntityId::from(2u64),
        edge_type: EdgeType::new("KNOWS"),
        properties: HashMap::new(),
    };
    let edge_key3 = 102u64.to_be_bytes();
    tx.put("edges", &edge_key3, &edge3.encode()?)?;

    tx.commit()?;

    println!("Created test database at {} with 3 entities and 3 edges", path);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_db_path = "/tmp/manifold_test.redb";

    // Remove old test database if it exists
    let _ = std::fs::remove_file(test_db_path);

    // Create test database
    create_test_database(test_db_path)?;

    // Run Python test
    let python_script = format!(r#"
import duckdb

conn = duckdb.connect(config={{'allow_unsigned_extensions': True}})
conn.execute("LOAD 'build/debug/duckdb_manifold.duckdb_extension'")

print("\\n=== Testing manifold_entities ===")
result = conn.execute("SELECT * FROM manifold_entities('{}')")
print("Columns:", [desc[0] for desc in result.description])
for row in result.fetchall():
    print(row)

print("\\n=== Testing manifold_edges ===")
result = conn.execute("SELECT * FROM manifold_edges('{}')")
print("Columns:", [desc[0] for desc in result.description])
for row in result.fetchall():
    print(row)

print("\\n=== Query: Find people over 25 ===")
result = conn.execute("SELECT id, prop_name, prop_age FROM manifold_entities('{}') WHERE CAST(prop_age AS INTEGER) > 25")
for row in result.fetchall():
    print(row)

print("\\n=== Query: Find WORKS_AT edges ===")
result = conn.execute("SELECT source, target, prop_since FROM manifold_edges('{}') WHERE edge_type = 'WORKS_AT'")
for row in result.fetchall():
    print(row)

print("\\nAll tests passed!")
"#, test_db_path, test_db_path, test_db_path, test_db_path);

    let output = Command::new("./configure/venv/bin/python3")
        .arg("-c")
        .arg(&python_script)
        .current_dir("/Users/tom/Developer/projects/khitomer/components/duckdb-manifold")
        .output()?;

    println!("{}", String::from_utf8_lossy(&output.stdout));
    if !output.stderr.is_empty() {
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    }

    if !output.status.success() {
        return Err("Python test failed".into());
    }

    Ok(())
}
