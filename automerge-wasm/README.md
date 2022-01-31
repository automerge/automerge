## Automerge WASM Low Level Interface

This is a low level automerge library written in rust exporting a javascript API via WASM.  This low level api is the underpinning to the `automerge-js` library that reimplements the Automerge API via these low level functions.

### Static Functions

### Methods

  `doc.clone(actor?: string)` : Make a complete 

  `doc.free()` : deallocate WASM memory associated with a document

#[wasm_bindgen]
    pub fn free(self) {}

    #[wasm_bindgen(js_name = pendingOps)]
    pub fn pending_ops(&self) -> JsValue {
        (self.0.pending_ops() as u32).into()
    }

    pub fn commit(&mut self, message: Option<String>, time: Option<f64>) -> Array {
        let heads = self.0.commit(message, time.map(|n| n as i64));
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        heads
    }

    pub fn rollback(&mut self) -> f64 {
        self.0.rollback() as f64
    }

    pub fn keys(&mut self, obj: String, heads: Option<Array>) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = if let Some(heads) = get_heads(heads) {
            self.0.keys_at(&obj, &heads)
        } else {
            self.0.keys(&obj)
        }
        .iter()
        .map(|s| JsValue::from_str(s))
        .collect();
        Ok(result)
    }

    pub fn text(&mut self, obj: String, heads: Option<Array>) -> Result<String, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            self.0.text_at(&obj, &heads)
        } else {
            self.0.text(&obj)
        }
        .map_err(to_js_err)
    }

    pub fn splice(
        &mut self,
        obj: String,
        start: f64,
        delete_count: f64,
        text: JsValue,
    ) -> Result<Option<Array>, JsValue> {
        let obj = self.import(obj)?;
        let start = start as usize;
        let delete_count = delete_count as usize;
        let mut vals = vec![];
        if let Some(t) = text.as_string() {
            self.0
                .splice_text(&obj, start, delete_count, &t)
                .map_err(to_js_err)?;
            Ok(None)
        } else {
            if let Ok(array) = text.dyn_into::<Array>() {
                for i in array.iter() {
                    if let Ok(array) = i.clone().dyn_into::<Array>() {
                        let value = array.get(1);
                        let datatype = array.get(2);
                        let value = self.import_value(value, datatype.as_string())?;
                        vals.push(value);
                    } else {
                        let value = self.import_value(i, None)?;
                        vals.push(value);
                    }
                }
            }
            let result = self
                .0
                .splice(&obj, start, delete_count, vals)
                .map_err(to_js_err)?;
            if result.is_empty() {
                Ok(None)
            } else {
                let result: Array = result
                    .iter()
                    .map(|r| JsValue::from(r.to_string()))
                    .collect();
                Ok(result.into())
            }
        }
    }

    pub fn push(
        &mut self,
        obj: String,
        value: JsValue,
        datatype: Option<String>,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let value = self.import_value(value, datatype)?;
        let index = self.0.length(&obj);
        let opid = self.0.insert(&obj, index, value).map_err(to_js_err)?;
        Ok(opid.map(|id| id.to_string()))
    }

    pub fn insert(
        &mut self,
        obj: String,
        index: f64,
        value: JsValue,
        datatype: Option<String>,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let index = index as f64;
        let value = self.import_value(value, datatype)?;
        let opid = self
            .0
            .insert(&obj, index as usize, value)
            .map_err(to_js_err)?;
        Ok(opid.map(|id| id.to_string()))
    }

    pub fn set(
        &mut self,
        obj: String,
        prop: JsValue,
        value: JsValue,
        datatype: Option<String>,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value = self.import_value(value, datatype)?;
        let opid = self.0.set(&obj, prop, value).map_err(to_js_err)?;
        Ok(opid.map(|id| id.to_string()))
    }

    pub fn make(
        &mut self,
        obj: String,
        prop: JsValue,
        value: JsValue,
    ) -> Result<String, JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value = self.import_value(value, None)?;
        if value.is_object() {
          let opid = self.0.set(&obj, prop, value).map_err(to_js_err)?;
          Ok(opid.unwrap().to_string())
        } else {
          Err("invalid object type".into())
        }
    }

    pub fn inc(&mut self, obj: String, prop: JsValue, value: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value
            .as_f64()
            .ok_or("inc needs a numberic value")
            .map_err(to_js_err)?;
        self.0.inc(&obj, prop, value as i64).map_err(to_js_err)?;
        Ok(())
    }

    pub fn value(
        &mut self,
        obj: String,
        prop: JsValue,
        heads: Option<Array>,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(prop);
        let heads = get_heads(heads);
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.0.value_at(&obj, prop, &h)
            } else {
                self.0.value(&obj, prop)
            }
            .map_err(to_js_err)?;
            match value {
                Some((Value::Object(obj_type), obj_id)) => {
                    result.push(&obj_type.to_string().into());
                    result.push(&obj_id.to_string().into());
                }
                Some((Value::Scalar(value), _)) => {
                    result.push(&datatype(&value).into());
                    result.push(&ScalarValue(value).into());
                }
                None => {}
            }
        }
        Ok(result)
    }

    pub fn values(
        &mut self,
        obj: String,
        arg: JsValue,
        heads: Option<Array>,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = if let Some(heads) = get_heads(heads) {
                self.0.values_at(&obj, prop, &heads)
            } else {
                self.0.values(&obj, prop)
            }
            .map_err(to_js_err)?;
            for value in values {
                match value {
                    (Value::Object(obj_type), obj_id) => {
                        let sub = Array::new();
                        sub.push(&obj_type.to_string().into());
                        sub.push(&obj_id.to_string().into());
                        result.push(&sub.into());
                    }
                    (Value::Scalar(value), id) => {
                        let sub = Array::new();
                        sub.push(&datatype(&value).into());
                        sub.push(&ScalarValue(value).into());
                        sub.push(&id.to_string().into());
                        result.push(&sub.into());
                    }
                }
            }
        }
        Ok(result)
    }

    pub fn length(&mut self, obj: String, heads: Option<Array>) -> Result<f64, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.0.length_at(&obj, &heads) as f64)
        } else {
            Ok(self.0.length(&obj) as f64)
        }
    }

    pub fn del(&mut self, obj: String, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.0.del(&obj, prop).map_err(to_js_err)?;
        Ok(())
    }

    pub fn mark(
        &mut self,
        obj: JsValue,
        range: JsValue,
        name: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let re = Regex::new(r"([\[\(])(\d+)\.\.(\d+)([\)\]])").unwrap();
        let range = range.as_string().ok_or("range must be a string")?;
        let cap = re.captures_iter(&range).next().ok_or("range must be in the form of (start..end] or [start..end) etc... () for sticky, [] for normal")?;
        let start: usize = cap[2].parse().map_err(|_| to_js_err("invalid start"))?;
        let end: usize = cap[3].parse().map_err(|_| to_js_err("invalid end"))?;
        let start_sticky = &cap[1] == "(";
        let end_sticky = &cap[4] == ")";
        let name = name
            .as_string()
            .ok_or("invalid mark name")
            .map_err(to_js_err)?;
        let value = self.import_scalar(&value, datatype.as_string())?;
        self.0
            .mark(&obj, start, start_sticky, end, end_sticky, &name, value)
            .map_err(to_js_err)?;
        Ok(())
    }

    pub fn spans(&mut self, obj: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let text = self.0.text(&obj).map_err(to_js_err)?;
        let spans = self.0.spans(&obj).map_err(to_js_err)?;
        let mut last_pos = 0;
        let result = Array::new();
        for s in spans {
            let marks = Array::new();
            for m in s.marks {
                let mark = Array::new();
                mark.push(&m.0.into());
                mark.push(&datatype(&m.1).into());
                mark.push(&ScalarValue(m.1).into());
                marks.push(&mark.into());
            }
            let text_span = &text[last_pos..s.pos]; //.slice(last_pos, s.pos);
            if text_span.len() > 0 {
                result.push(&text_span.into());
            }
            result.push(&marks);
            last_pos = s.pos;
            //let obj = Object::new().into();
            //js_set(&obj, "pos", s.pos as i32)?;
            //js_set(&obj, "marks", marks)?;
            //result.push(&obj.into());
        }
        let text_span = &text[last_pos..];
        if text_span.len() > 0 {
            result.push(&text_span.into());
        }
        Ok(result.into())
    }

    pub fn save(&mut self) -> Result<Uint8Array, JsValue> {
        self.0
            .save()
            .map(|v| Uint8Array::from(v.as_slice()))
            .map_err(to_js_err)
    }

    #[wasm_bindgen(js_name = saveIncremental)]
    pub fn save_incremental(&mut self) -> Uint8Array {
        let bytes = self.0.save_incremental();
        Uint8Array::from(bytes.as_slice())
    }

    #[wasm_bindgen(js_name = loadIncremental)]
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<f64, JsValue> {
        let data = data.to_vec();
        let len = self.0.load_incremental(&data).map_err(to_js_err)?;
        Ok(len as f64)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<(), JsValue> {
        let changes: Vec<_> = JS(changes).try_into()?;
        self.0.apply_changes(&changes).map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: JsValue) -> Result<Array, JsValue> {
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.0.get_changes(&deps);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &Automerge) -> Result<Array, JsValue> {
        let changes = self.0.get_changes_added(&other.0);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&mut self) -> Array {
        let heads = self.0.get_heads();
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        heads
    }

    #[wasm_bindgen(js_name = getActorId)]
    pub fn get_actor_id(&mut self) -> String {
        let actor = self.0.get_actor();
        actor.to_string()
    }

    #[wasm_bindgen(js_name = getLastLocalChange)]
    pub fn get_last_local_change(&mut self) -> Result<Option<Uint8Array>, JsValue> {
        if let Some(change) = self.0.get_last_local_change() {
            Ok(Some(Uint8Array::from(change.raw_bytes())))
        } else {
            Ok(None)
        }
    }

    pub fn dump(&self) {
        self.0.dump()
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&mut self, heads: Option<Array>) -> Result<Array, JsValue> {
        let heads = get_heads(heads).unwrap_or_default();
        let deps = self.0.get_missing_deps(&heads);
        let deps: Array = deps
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        Ok(deps)
    }

    #[wasm_bindgen(js_name = receiveSyncMessage)]
    pub fn receive_sync_message(
        &mut self,
        state: &mut SyncState,
        message: Uint8Array,
    ) -> Result<(), JsValue> {
        let message = message.to_vec();
        let message = am::SyncMessage::decode(message.as_slice()).map_err(to_js_err)?;
        self.0
            .receive_sync_message(&mut state.0, message)
            .map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = generateSyncMessage)]
    pub fn generate_sync_message(&mut self, state: &mut SyncState) -> Result<JsValue, JsValue> {
        if let Some(message) = self.0.generate_sync_message(&mut state.0) {
            Ok(Uint8Array::from(message.encode().map_err(to_js_err)?.as_slice()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = toJS)]
    pub fn to_js(&self) -> JsValue {
        map_to_js(&self.0, &ROOT)
    }

    fn import(&self, id: String) -> Result<ObjId, JsValue> {
        self.0.import(&id).map_err(to_js_err)
    }

    fn import_prop(&mut self, prop: JsValue) -> Result<Prop, JsValue> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(format!("invalid prop {:?}", prop).into())
        }
    }

    fn import_scalar(
        &mut self,
        value: &JsValue,
        datatype: Option<String>,
    ) -> Result<am::ScalarValue, JsValue> {
        match datatype.as_deref() {
            Some("boolean") => value
                .as_bool()
                .ok_or_else(|| "value must be a bool".into())
                .map(am::ScalarValue::Boolean),
            Some("int") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Int(v as i64)),
            Some("uint") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Uint(v as u64)),
            Some("f64") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(am::ScalarValue::F64),
            Some("bytes") => Ok(am::ScalarValue::Bytes(
                value.clone().dyn_into::<Uint8Array>().unwrap().to_vec(),
            )),
            Some("counter") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::counter(v as i64)),
            Some("timestamp") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Timestamp(v as i64)),
            /*
            Some("bytes") => unimplemented!(),
            Some("cursor") => unimplemented!(),
            */
            Some("null") => Ok(am::ScalarValue::Null),
            Some(_) => Err(format!("unknown datatype {:?}", datatype).into()),
            None => {
                if value.is_null() {
                    Ok(am::ScalarValue::Null)
                } else if let Some(b) = value.as_bool() {
                    Ok(am::ScalarValue::Boolean(b))
                } else if let Some(s) = value.as_string() {
                    // FIXME - we need to detect str vs int vs float vs bool here :/
                    Ok(am::ScalarValue::Str(s.into()))
                } else if let Some(n) = value.as_f64() {
                    if (n.round() - n).abs() < f64::EPSILON {
                        Ok(am::ScalarValue::Int(n as i64))
                    } else {
                        Ok(am::ScalarValue::F64(n))
                    }
                //                } else if let Some(o) = to_objtype(&value) {
                //                    Ok(o.into())
                } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                    Ok(am::ScalarValue::Timestamp(d.get_time() as i64))
                } else if let Ok(o) = &value.clone().dyn_into::<Uint8Array>() {
                    Ok(am::ScalarValue::Bytes(o.to_vec()))
                } else {
                    Err("value is invalid".into())
                }
            }
        }
    }

    fn import_value(&mut self, value: JsValue, datatype: Option<String>) -> Result<Value, JsValue> {
        match self.import_scalar(&value, datatype) {
            Ok(val) => Ok(val.into()),
            Err(err) => {
                if let Some(o) = to_objtype(&value) {
                    Ok(o.into())
                } else {
                    Err(err)
                }
            }
        }
        /*
        match datatype.as_deref() {
            Some("boolean") => value
                .as_bool()
                .ok_or_else(|| "value must be a bool".into())
                .map(|v| am::ScalarValue::Boolean(v).into()),
            Some("int") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Int(v as i64).into()),
            Some("uint") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Uint(v as u64).into()),
            Some("f64") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|n| am::ScalarValue::F64(n).into()),
            Some("bytes") => {
                Ok(am::ScalarValue::Bytes(value.dyn_into::<Uint8Array>().unwrap().to_vec()).into())
            }
            Some("counter") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::counter(v as i64).into()),
            Some("timestamp") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Timestamp(v as i64).into()),
            Some("null") => Ok(am::ScalarValue::Null.into()),
            Some(_) => Err(format!("unknown datatype {:?}", datatype).into()),
            None => {
                if value.is_null() {
                    Ok(am::ScalarValue::Null.into())
                } else if let Some(b) = value.as_bool() {
                    Ok(am::ScalarValue::Boolean(b).into())
                } else if let Some(s) = value.as_string() {
                    // FIXME - we need to detect str vs int vs float vs bool here :/
                    Ok(am::ScalarValue::Str(s.into()).into())
                } else if let Some(n) = value.as_f64() {
                    if (n.round() - n).abs() < f64::EPSILON {
                        Ok(am::ScalarValue::Int(n as i64).into())
                    } else {
                        Ok(am::ScalarValue::F64(n).into())
                    }
                } else if let Some(o) = to_objtype(&value) {
                    Ok(o.into())
                } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                    Ok(am::ScalarValue::Timestamp(d.get_time() as i64).into())
                } else if let Ok(o) = &value.dyn_into::<Uint8Array>() {
                    Ok(am::ScalarValue::Bytes(o.to_vec()).into())
                } else {
                    Err("value is invalid".into())
                }
            }
        }
        */
    }
}

#[wasm_bindgen(js_name = create)]
pub fn init(actor: Option<String>) -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen(js_name = loadDoc)]
pub fn load(data: Uint8Array, actor: Option<String>) -> Result<Automerge, JsValue> {
    let data = data.to_vec();
    let mut automerge = am::Automerge::load(&data).map_err(to_js_err)?;
    if let Some(s) = actor {
        let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
        automerge.set_actor(actor)
    }
    Ok(Automerge(automerge))
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, JsValue> {
    let change: am::ExpandedChange = change.into_serde().map_err(to_js_err)?;
    let change: Change = change.into();
    Ok(Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange)]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, JsValue> {
    let change = Change::from_bytes(change.to_vec()).map_err(to_js_err)?;
    let change: am::ExpandedChange = change.decode();
    JsValue::from_serde(&change).map_err(to_js_err)
}

#[wasm_bindgen(js_name = initSyncState)]
pub fn init_sync_state() -> SyncState {
    SyncState(am::SyncState::new())
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = importSyncState)]
pub fn import_sync_state(state: JsValue) -> Result<SyncState, JsValue> {
    Ok(SyncState(JS(state).try_into()?))
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = exportSyncState)]
pub fn export_sync_state(state: SyncState) -> JsValue {
    JS::from(state.0).into()
}

#[wasm_bindgen(js_name = encodeSyncMessage)]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, JsValue> {
    let heads = js_get(&message, "heads")?.try_into()?;
    let need = js_get(&message, "need")?.try_into()?;
    let changes = js_get(&message, "changes")?.try_into()?;
    let have = js_get(&message, "have")?.try_into()?;
    Ok(Uint8Array::from(
        am::SyncMessage {
            heads,
            need,
            have,
            changes,
        }
        .encode()
        .unwrap()
        .as_slice(),
    ))
}

#[wasm_bindgen(js_name = decodeSyncMessage)]
pub fn decode_sync_message(msg: Uint8Array) -> Result<JsValue, JsValue> {
    let data = msg.to_vec();
    let msg = am::SyncMessage::decode(&data).map_err(to_js_err)?;
    let heads = AR::from(msg.heads.as_slice());
    let need = AR::from(msg.need.as_slice());
    let changes = AR::from(msg.changes.as_slice());
    let have = AR::from(msg.have.as_slice());
    let obj = Object::new().into();
    js_set(&obj, "heads", heads)?;
    js_set(&obj, "need", need)?;
    js_set(&obj, "have", have)?;
    js_set(&obj, "changes", changes)?;
    Ok(obj)
}

#[wasm_bindgen(js_name = encodeSyncState)]
pub fn encode_sync_state(state: SyncState) -> Result<Uint8Array, JsValue> {
    let state = state.0;
    Ok(Uint8Array::from(
        state.encode().map_err(to_js_err)?.as_slice(),
    ))
}

#[wasm_bindgen(js_name = decodeSyncState)]
pub fn decode_sync_state(data: Uint8Array) -> Result<SyncState, JsValue> {
    SyncState::decode(data)
}

#[wasm_bindgen(js_name = MAP)]
pub struct Map {}

#[wasm_bindgen(js_name = LIST)]
pub struct List {}

#[wasm_bindgen(js_name = TEXT)]
pub struct Text {}

#[wasm_bindgen(js_name = TABLE)]
pub struct Table {}
