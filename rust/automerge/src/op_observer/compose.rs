use super::ObserverContext;
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
        ctx: ObserverContext,
        objid: crate::ObjId,
        index: usize,
        tagged_value: (crate::Value<'_>, crate::ObjId),
        conflict: bool,
    ) {
        self.obs1.insert(
            doc,
            ctx,
            objid.clone(),
            index,
            tagged_value.clone(),
            conflict,
        );
        self.obs2
            .insert(doc, ctx, objid, index, tagged_value, conflict);
    }

    fn splice_text<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        index: usize,
        value: &str,
    ) {
        self.obs1.splice_text(doc, ctx, objid.clone(), index, value);
        self.obs2.splice_text(doc, ctx, objid, index, value);
    }

    fn put<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        prop: crate::Prop,
        tagged_value: (crate::Value<'_>, crate::ObjId),
        conflict: bool,
    ) {
        self.obs1.put(
            doc,
            ctx,
            objid.clone(),
            prop.clone(),
            tagged_value.clone(),
            conflict,
        );
        self.obs2.put(doc, ctx, objid, prop, tagged_value, conflict);
    }

    fn expose<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        prop: crate::Prop,
        tagged_value: (crate::Value<'_>, crate::ObjId),
        conflict: bool,
    ) {
        self.obs1.expose(
            doc,
            ctx,
            objid.clone(),
            prop.clone(),
            tagged_value.clone(),
            conflict,
        );
        self.obs2
            .expose(doc, ctx, objid, prop, tagged_value, conflict);
    }

    fn increment<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        prop: crate::Prop,
        tagged_value: (i64, crate::ObjId),
    ) {
        self.obs1
            .increment(doc, ctx, objid.clone(), prop.clone(), tagged_value.clone());
        self.obs2.increment(doc, ctx, objid, prop, tagged_value);
    }

    fn mark<'b, R: crate::ReadDoc, M: Iterator<Item = crate::marks::Mark<'b>>>(
        &mut self,
        doc: &'b R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        mark: M,
    ) {
        let marks: Vec<_> = mark.collect();
        self.obs1
            .mark(doc, ctx, objid.clone(), marks.clone().into_iter());
        self.obs2.mark(doc, ctx, objid, marks.into_iter());
    }

    fn unmark<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        name: &str,
        start: usize,
        end: usize,
    ) {
        self.obs1.unmark(doc, ctx, objid.clone(), name, start, end);
        self.obs2.unmark(doc, ctx, objid, name, start, end);
    }

    fn delete_map<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        key: &str,
    ) {
        self.obs1.delete_map(doc, ctx, objid.clone(), key);
        self.obs2.delete_map(doc, ctx, objid, key);
    }

    fn delete_seq<R: crate::ReadDoc>(
        &mut self,
        doc: &R,
        ctx: ObserverContext,
        objid: crate::ObjId,
        index: usize,
        num: usize,
    ) {
        self.obs2.delete_seq(doc, ctx, objid.clone(), index, num);
        self.obs2.delete_seq(doc, ctx, objid, index, num);
    }
}
