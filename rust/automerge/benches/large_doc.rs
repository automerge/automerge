use automerge::{transaction::Transactable, AutoCommit, ROOT};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn doc(n: u64) -> AutoCommit {
    let mut doc = AutoCommit::new();
    let list = doc
        .put_object(ROOT, "my list", automerge::ObjType::List)
        .unwrap();
    /*
        {
      "uid": 10000,
      "components": {
        "StaticMapEntity": {
          "origin": {
            "x": -2,
            "y": -2
          },
          "rotation": 0,
          "originalRotation": 0,
          "code": 26
        },
        "ItemProcessor": {
          "nextOutputSlot": 0
        },
        "WiredPins": {
          "slots": [
            {
              "value": {
                "$": "shape",
                "data": "Sg----Sg:CgCgCgCg:--CyCy--"
              }
            }
          ]
        }
      }
    }
         */
    for i in 0..n {
        let map = doc
            .insert_object(&list, i as usize, automerge::ObjType::Map)
            .unwrap();
        doc.put(&map, "uid", 10_000).unwrap();
        let components = doc
            .put_object(&map, "components", automerge::ObjType::Map)
            .unwrap();
        let static_map_entity = doc
            .put_object(&components, "StaticMapEntity", automerge::ObjType::Map)
            .unwrap();
        let origin = doc
            .put_object(&static_map_entity, "origin", automerge::ObjType::Map)
            .unwrap();
        doc.put(&origin, "x", -2).unwrap();
        doc.put(&origin, "y", -2).unwrap();
        doc.put(&static_map_entity, "rotation", 0).unwrap();
        let _original_rotation: () = doc.put(&static_map_entity, "originialRotation", 0).unwrap();
        doc.put(&static_map_entity, "code", 26).unwrap();
        let item_processor = doc
            .put_object(&components, "ItemProcessor", automerge::ObjType::Map)
            .unwrap();
        doc.put(&item_processor, "nextOutputSlot", 0).unwrap();
        let wired_pins = doc
            .put_object(&components, "WiredPins", automerge::ObjType::Map)
            .unwrap();
        let slots = doc
            .put_object(&wired_pins, "slots", automerge::ObjType::List)
            .unwrap();
        let slot = doc
            .insert_object(&slots, 0, automerge::ObjType::Map)
            .unwrap();
        let value = doc
            .put_object(&slot, "value", automerge::ObjType::Map)
            .unwrap();
        doc.put(&value, "$", "shape").unwrap();
        doc
            .put(&value, "data", "Sg----Sg:CgCgCgCg:--CyCy--")
            .unwrap();
    }
    doc.commit().unwrap();
    doc
}

fn criterion_benchmark(c: &mut Criterion) {
    let n = 100_000;
    c.bench_function(&format!("large_doc {}", n), |b| {
        b.iter(|| black_box(doc(n)));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
