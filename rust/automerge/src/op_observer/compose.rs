use super::OpObserver;

pub fn compose<'a, O1: OpObserver, O2: OpObserver>(
    obs1: &'a mut O1,
    obs2: &'a mut O2,
) -> impl OpObserver + 'a {
    ComposeObservers { obs1, obs2 }
}

struct ComposeObservers<'a, O1: OpObserver, O2: OpObserver> {
    obs1: &'a mut O1,
    obs2: &'a mut O2,
}

impl<'a, O1: OpObserver, O2: OpObserver> OpObserver for ComposeObservers<'a, O1, O2> {
    fn insert<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        objid: crate::ObjId,
        index: usize,
        tagged_value: (crate::Value<'_>, crate::ObjId),
    ) {
        self.obs1
            .insert(doc, objid.clone(), index, tagged_value.clone());
        self.obs2.insert(doc, objid, index, tagged_value);
    }

    fn splice_text<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        objid: crate::ObjId,
        index: usize,
        value: &str,
    ) {
        self.obs1.splice_text(doc, objid.clone(), index, value);
        self.obs2.splice_text(doc, objid, index, value);
    }

    fn put<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        objid: crate::ObjId,
        prop: crate::Prop,
        tagged_value: (crate::Value<'_>, crate::ObjId),
        conflict: bool,
    ) {
        self.obs1.put(
            doc,
            objid.clone(),
            prop.clone(),
            tagged_value.clone(),
            conflict,
        );
        self.obs2.put(doc, objid, prop, tagged_value, conflict);
    }

    fn expose<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        objid: crate::ObjId,
        prop: crate::Prop,
        tagged_value: (crate::Value<'_>, crate::ObjId),
        conflict: bool,
    ) {
        self.obs1.expose(
            doc,
            objid.clone(),
            prop.clone(),
            tagged_value.clone(),
            conflict,
        );
        self.obs2.expose(doc, objid, prop, tagged_value, conflict);
    }

    fn increment<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        objid: crate::ObjId,
        prop: crate::Prop,
        tagged_value: (i64, crate::ObjId),
    ) {
        self.obs1
            .increment(doc, objid.clone(), prop.clone(), tagged_value.clone());
        self.obs2.increment(doc, objid, prop, tagged_value);
    }

    fn delete_map<R: crate::ReadDoc>(&mut self, doc: &R, objid: crate::ObjId, key: &str) {
        self.obs1.delete_map(doc, objid.clone(), key);
        self.obs2.delete_map(doc, objid, key);
    }

    fn delete_seq<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        objid: crate::ObjId,
        index: usize,
        num: usize,
    ) {
        self.obs2.delete_seq(doc, objid.clone(), index, num);
        self.obs2.delete_seq(doc, objid, index, num);
    }
}
