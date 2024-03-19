use crate::{hydrate, Block};

pub(crate) fn hydrate_block(value: hydrate::Value) -> Option<Block> {
    let hydrate::Value::Map(mut map) = value else {
        tracing::warn!("Expected map, got {:?}", value);
        return None;
    };
    let block_type = map.get("type")?;
    let hydrate::Value::Scalar(crate::ScalarValue::Str(block_type)) = block_type else {
        tracing::warn!("Expected block_type, got {:?}", block_type);
        return None;
    };
    let block_type = block_type.to_string();
    let parents = map.get("parents")?;
    let hydrate::Value::List(parents) = parents else {
        tracing::warn!("Expected parents, got {:?}", parents);
        return None;
    };
    let parents = parents
        .iter()
        .filter_map(|p| match &p.value {
            hydrate::Value::Scalar(crate::ScalarValue::Str(p)) => Some(p.to_string()),
            _ => None,
        })
        .collect();

    let block = Block::new(block_type).with_parents(parents);

    let attrs = map.remove("attrs").map(|a| a.value);
    match attrs {
        Some(hydrate::Value::Map(attrs)) => {
            let attrs = attrs
                .iter()
                .filter_map(|(k, v)| match &v.value {
                    hydrate::Value::Scalar(v) => Some((k.clone(), v.clone())),
                    _ => None,
                });
            Some(block.with_attrs(attrs))
        },
        _ => Some(block)
    }
}


