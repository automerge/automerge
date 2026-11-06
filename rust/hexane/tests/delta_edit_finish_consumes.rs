//! `DeltaEdit::finish(&mut self)` paid the fix-up the cursor's edits owe
//! by rewriting the next value, which moved the cursor past it; the
//! cursor stayed usable, and an edit after `finish` landed one item late:
//! on `[10, 20, 30]`, `seek(1).insert(15)`, `finish()`, `insert(17)` gave
//! `[10, 15, 20, 17, 30]`. `Column`'s cursor, which owes nothing, stays
//! put. Now `finish` takes the cursor, so an edit after it doesn't
//! compile (the doctest on `finish` checks that); edits before it, and
//! dropping the cursor instead, work as before.

use hexane::DeltaColumn;

#[test]
fn finish_closes_the_cursor() {
    let mut col = DeltaColumn::<i64>::from_values(vec![10, 20, 30]);
    let mut e = col.edit();
    e.seek(1).insert(15).insert(17);
    e.finish();
    assert_eq!(col.to_vec(), vec![10, 15, 17, 20, 30]);
}

#[test]
fn dropping_the_cursor_does_the_same() {
    let mut col = DeltaColumn::<Option<i64>>::from_values(vec![Some(10), None, Some(30)]);
    {
        let mut e = col.edit();
        e.seek(1).delete(1).insert(Some(25));
    }
    assert_eq!(col.to_vec(), vec![Some(10), Some(25), Some(30)]);
}
