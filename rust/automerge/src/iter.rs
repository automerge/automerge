use crate::types::{Clock, Key, Op};

pub(crate) struct TopOps<'a, I: Iterator<Item = &'a Op>> {
    iter: I,
    pos: usize,
    start_pos: usize,
    num_ops: usize,
    clock: Option<Clock>,
    key: Option<Key>,
    last_op: Option<(usize, &'a Op)>,
}

impl<'a, I: Iterator<Item = &'a Op>> TopOps<'a, I> {
    pub(crate) fn new(iter: I, clock: Option<Clock>) -> Self {
        Self {
            iter,
            pos: 0,
            start_pos: 0,
            num_ops: 0,
            clock,
            key: None,
            last_op: None,
        }
    }
}

pub(crate) struct TopOp<'a> {
    pub(crate) op: &'a Op,
    //pub(crate) op_pos: usize,
    //pub(crate) start_pos: usize,
    //pub(crate) num_ops: usize,
}

impl<'a, I: Iterator<Item = &'a Op>> Iterator for TopOps<'a, I> {
    type Item = TopOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;
        loop {
            if let Some(op) = self.iter.next() {
                let key = op.elemid_or_key();
                let visible = op.visible_at(self.clock.as_ref());
                match &self.key {
                    Some(k) if k == &key => {
                        if visible {
                            self.last_op = Some((self.pos, op));
                        }
                        self.num_ops += 1;
                    }
                    Some(_) => {
                        result = self.last_op.take().map(|(_op_pos, op)| TopOp {
                            op,
                            //op_pos,
                            //start_pos: self.start_pos,
                            //num_ops: self.num_ops,
                        });
                        if visible {
                            self.last_op = Some((self.pos, op));
                        }
                        self.key = Some(key);
                        self.start_pos = self.pos;
                        self.num_ops = 1;
                    }
                    None => {
                        self.key = Some(key);
                        self.start_pos = self.pos;
                        self.num_ops = 1;
                        if visible {
                            self.last_op = Some((self.pos, op));
                        }
                    }
                }
                self.pos += 1;
                if result.is_some() {
                    break;
                }
            } else {
                result = self.last_op.take().map(|(_op_pos, op)| TopOp {
                    op,
                    //op_pos,
                    //start_pos: self.start_pos,
                    //num_ops: self.num_ops,
                });
                break;
            }
        }
        result
    }
}
