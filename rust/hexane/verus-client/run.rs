//! Runs the clients Verus checked: each call meets its client's
//! `requires`, and the client's own run-time work (loading, saving,
//! unwrapping) must not panic.
use hexane::verified::{
    AccIndex, Get, LenIndex, SumIndex, VCodec, VDeltaValue, VIndex, VValue, VerifiedAccColumn,
    VerifiedAccIter, VerifiedAccValue, VerifiedColumn, VerifiedDecoder, VerifiedDeltaColumn,
    VerifiedEncoder, VerifiedEncoderState, VerifiedPrefixColumn,
};
use hexane::{DeltaRun, Leb128, Run};
use verus_client::{Action, Board, Flags, Mark, Meta, Step};

#[test]
fn runs() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    verus_client::round_trip_u64(&v);
    verus_client::round_trip_nullable(&vec![Some(-1), None, None, Some(i64::MIN)]);
    verus_client::round_trip_bool(&vec![true, false, false]);

    let acts: Vec<Option<Action>> = (0..1000)
        .map(|i| match i % 7 {
            0 => Some(Action::Make),
            1 | 2 => Some(Action::Set),
            3 => None,
            4 => Some(Action::Delete),
            _ => Some(Action::Increment),
        })
        .collect();
    let plain: Vec<Action> = acts.iter().flatten().copied().collect();
    verus_client::round_trip_action(&plain, &acts);
    verus_client::edit_and_read(Action::Make, Action::Increment);
    let mut c = VerifiedColumn::<Option<Action>>::new();
    c.splice(0, 0, &acts);
    let bytes = c.save();

    verus_client::round_trip_bytes(&vec![&b"ab"[..], b"", b"ab"]);
    verus_client::bytes_in_bytes_out(b"ab", b"");
    verus_client::bytes_or_null(b"x", b"");
    verus_client::load_then_save::<Option<Action>, Leb128, LenIndex>(&bytes);
    verus_client::load_then_save::<u64, Leb128, LenIndex>(&[0x7f]);
    verus_client::load_then_save::<bool, Leb128, LenIndex>(&[3, 2]);
    for b in [&bytes[..], &[0x7f], &[3, 2], &[]] {
        let plain = VerifiedColumn::<u64>::load(b).ok().map(|c| c.to_vec());
        let of_a_kind = verus_client::load_as_a_kind::<u64, Leb128, LenIndex>(b);
        assert_eq!(of_a_kind.map(|c| c.to_vec()), plain);
        assert_eq!(
            verus_client::load_plain::<u64, Leb128>(b).map(|c| c.to_vec()),
            plain
        );
    }
    let back = verus_client::load_plain::<Option<Action>, Leb128>(&bytes).unwrap();
    assert_eq!(back.save(), bytes);
    verus_client::count_and_cut(&vec![7, 7, 8, 9]);

    verus_client::two_in_one_buffer::<u64, Leb128>(&v, &vec![9; 40]);
    verus_client::two_in_one_buffer::<Vec<u8>, Leb128>(&vec![&b"ab"[..], b""], &vec![]);
    verus_client::one_value_only::<u64, Leb128>(1, 2);
    verus_client::one_value_only::<Option<Vec<u8>>, Leb128>(Some(b"x"), None);
    verus_client::only_nulls(b"x", b"");
    verus_client::clone_then_diverge::<Vec<u8>, Leb128, LenIndex>(&vec![&b"ab"[..], b""], b"c");
    verus_client::clone_then_diverge::<Option<Action>, Leb128, LenIndex>(&acts, None);
    verus_client::clone_of_clone(b"xy");

    verus_client::collect_then_extend::<u64, Leb128, LenIndex>(vec![1, 1, 2], vec![3], vec![4, 4]);
    verus_client::collect_then_extend::<Option<Vec<u8>>, Leb128, LenIndex>(
        vec![Some(&b"a"[..]), None],
        vec![],
        vec![Some(b"")],
    );
    verus_client::collect_each(
        v.clone(),
        vec![&b"ab"[..], b""],
        vec![None, Some(b"x")],
        vec![b"c"],
    );
    verus_client::runs_in::<u64, Leb128, LenIndex>(
        1,
        2,
        3,
        vec![Run { count: 3, value: 9 }, Run { count: 0, value: 8 }],
    );
    verus_client::runs_in::<Vec<u8>, Leb128, LenIndex>(
        b"a",
        b"b",
        b"a",
        vec![Run {
            count: 2,
            value: b"",
        }],
    );
    verus_client::runs_of_slices(b"xy", 70_000, vec![Run { count: 5, value: 1 }], vec![]);

    verus_client::load_with_a_length::<u64, Leb128>(&v, 7, 3);
    verus_client::load_with_a_length::<u64, Leb128>(&vec![], 7, 3);
    verus_client::load_with_a_length::<Option<Vec<u8>>, Leb128>(
        &vec![Some(&b"a"[..]), None],
        None,
        usize::MAX,
    );
    for n in [0, 1, 100, 70_000, usize::MAX - 3, usize::MAX] {
        verus_client::load_a_fill::<u64, Leb128>(7, n);
        verus_client::load_a_fill::<bool, Leb128>(true, n);
        verus_client::load_a_fill::<Vec<u8>, Leb128>(b"ab", n);
        verus_client::load_a_fill::<Option<Action>, Leb128>(None, n);
    }
    verus_client::load_a_fill::<i64, Leb128>(-1, i64::MAX as u64 as usize);
    verus_client::load_fills(b"xyz");

    verus_client::slabs_and_validation::<u64, Leb128, LenIndex>(&(0..10_000).collect());
    verus_client::slabs_and_validation::<Option<Vec<u8>>, Leb128, LenIndex>(&vec![
        Some(&b"a"[..]),
        None,
    ]);
    verus_client::slabs_of_each(&v, &vec![true, false, false]);
    verus_client::load_then_validate::<Option<Action>, Leb128, LenIndex>(&bytes);
    verus_client::load_then_validate::<u64, Leb128, LenIndex>(&[0x7f]);

    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    verus_client::remap_as_it_is(&mut col);
    assert_eq!(col.to_vec(), v);
    verus_client::double_the_small(&mut col);
    assert!(col.to_vec().iter().zip(&v).all(|(a, b)| *a == 2 * *b));
    let mut a = VerifiedColumn::<Action>::new();
    a.splice(0, 0, &plain);
    let mut n = VerifiedColumn::<Option<Action>>::new();
    n.splice(0, 0, &acts);
    verus_client::step_the_actions(&mut a, &mut n);
    assert_eq!((a.len(), n.len()), (plain.len(), acts.len()));
    let mut b = VerifiedColumn::<Vec<u8>>::new();
    b.splice(0, 0, &vec![&b"ab"[..], b"", b"ab"]);
    let mut ob = VerifiedColumn::<Option<Vec<u8>>>::new();
    ob.splice(0, 0, &vec![Some(&b"a"[..]), None]);
    verus_client::longer_and_emptied(&mut b, &mut ob, b'!');
    assert_eq!(b.to_vec(), vec![&b"ab!"[..], b"!", b"ab!"]);
    assert_eq!(ob.to_vec(), vec![Some(&b""[..]), None]);
    let mut flags = VerifiedColumn::<bool>::new();
    flags.splice(0, 0, &vec![true, false, false]);
    let mut nums = VerifiedColumn::<i64>::new();
    nums.splice(0, 0, &vec![-5, 3, i64::MAX]);
    verus_client::turned_and_raised(&mut flags, &mut nums, 0);
    assert_eq!(flags.to_vec(), vec![false, true, true]);
    assert!(nums
        .to_vec()
        .iter()
        .zip([-5, 3, i64::MAX])
        .all(|(a, b)| *a >= b && *a >= 0));
    let mut o = VerifiedColumn::<Option<u64>>::new();
    o.splice(0, 0, &vec![Some(1), None, Some(u64::MAX - 1)]);
    verus_client::one_more(&mut o);
    assert_eq!(o.to_vec(), vec![Some(2), None, Some(u64::MAX)]);
}

#[test]
fn iterators() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    let mut b = VerifiedColumn::<Vec<u8>>::new();
    b.splice(0, 0, &vec![&b"ab"[..], b"", b"", b"ab"]);
    let mut ten = VerifiedColumn::<u64>::new();
    ten.splice(0, 0, &(0..10).collect::<Vec<u64>>());

    assert_eq!(verus_client::read_in_a_loop(&col), v);
    let mut small = VerifiedColumn::<u32>::new();
    small.splice(0, 0, &vec![1, 5, 3]);
    assert!(verus_client::all_below(&small, 6) && !verus_client::all_below(&small, 5));
    assert!(verus_client::agree(&col, &col.clone()));
    let mut five = VerifiedColumn::<i64>::new();
    five.splice(0, 0, &vec![-2, -1, 0, 1, 2]);
    verus_client::every_other(&five);
    assert_eq!(verus_client::the_third(&col), Some(0));
    assert_eq!(verus_client::the_third(&b), Some(&b""[..]));
    assert_eq!(verus_client::the_greatest(&col), 333);
    let mut three = VerifiedColumn::<u64>::new();
    three.splice(0, 0, &vec![4, 4, 5]);
    verus_client::two_readers(&three);
    assert_eq!(verus_client::in_the_range(&col, 10, 20), v[10..20]);
    verus_client::where_it_stands(&ten);
    assert_eq!(verus_client::read_out(&col, 10, 2000), (990, 1000, 1000));
    assert_eq!(verus_client::read_out(&col, 10, 20), (10, 20, 20));
    assert_eq!(verus_client::at_two_places(&col, 3, 30, 5), (1, 10, 4));
    let it = col.iter_range(5, 50);
    assert_eq!(verus_client::window_len(&it), 45);
    assert!(verus_client::ends_within(&it, 1000));
    verus_client::moved_past(&ten);
    verus_client::told_by_the_call(&ten);

    assert_eq!(
        verus_client::the_first_run(&col),
        Some(Run { count: 3, value: 0 })
    );
    assert_eq!(
        verus_client::the_first_run(&VerifiedColumn::<u64>::new()),
        None
    );
    verus_client::runs_in_a_window::<u64, Leb128, LenIndex>(1, 2);
    verus_client::runs_in_a_window::<Option<Vec<u8>>, Leb128, LenIndex>(None, Some(b""));
    let runs = verus_client::in_runs(&col);
    assert_eq!(runs.len(), 334);
    assert!(runs.iter().take(333).all(|r| r.count == 3) && runs[333].count == 1);
    assert_eq!(
        verus_client::in_runs(&b),
        vec![
            Run {
                count: 1,
                value: &b"ab"[..]
            },
            Run {
                count: 2,
                value: b""
            },
            Run {
                count: 1,
                value: b"ab"
            }
        ]
    );
    assert_eq!(verus_client::count_by_runs(&col, 10, 500), 490);
    assert_eq!(
        verus_client::copied_by_runs::<_, _, _, LenIndex>(&col).to_vec(),
        v
    );
    assert_eq!(
        verus_client::copied_by_runs::<_, _, _, LenIndex>(&b).to_vec(),
        b.to_vec()
    );
    let mut flags = VerifiedColumn::<bool>::new();
    flags.splice(0, 0, &vec![true, true, false]);
    verus_client::run_by_run(&flags);
    let first = Some(Run {
        count: 2,
        value: true,
    });
    assert_eq!(verus_client::told_by_a_run(flags.iter()), first);
    assert_eq!(verus_client::told_by_the_runs(flags.iter()), first);
    assert_eq!(verus_client::told_by_a_run(flags.iter_range(3, 3)), None);
    assert_eq!(
        verus_client::told_by_the_runs(flags.iter_range(1, 2)),
        Some(Run {
            count: 1,
            value: true
        })
    );
    assert_eq!(
        verus_client::told_by_a_run(col.iter_range(4, 900)),
        Some(Run { count: 2, value: 1 })
    );
}

#[test]
fn searches() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    let mut b = VerifiedColumn::<Vec<u8>>::new();
    b.splice(0, 0, &vec![&b"ab"[..], b"", b"", b"ab"]);

    assert_eq!(verus_client::told_by_a_scan(col.iter(), 7), Some(21));
    assert_eq!(
        verus_client::told_by_a_scan(col.iter_range(22, 40), 7),
        Some(22)
    );
    assert_eq!(
        verus_client::told_by_a_scan(col.iter_range(30, 40), 7),
        None
    );
    assert_eq!(verus_client::told_by_a_scan(b.iter(), &b""[..]), Some(1));
    assert_eq!(verus_client::told_by_a_seek(col.iter(), 7, 0..1000), 21..24);
    assert_eq!(
        verus_client::told_by_a_seek(col.iter_range(10, 500), 7, 22..400),
        22..24
    );
    assert_eq!(verus_client::first_place_in(&col, 7, 0, 1000), Some(21));
    assert_eq!(verus_client::first_place_in(&col, 7, 22, 1000), Some(22));
    assert_eq!(verus_client::first_place_in(&col, 7, 24, 1000), None);
    assert_eq!(verus_client::all_places(&col, 7), vec![21, 22, 23]);
    assert_eq!(verus_client::all_places(&b, &b"ab"[..]), vec![0, 3]);
    assert_eq!(verus_client::all_places(&b, &b"x"[..]), Vec::<usize>::new());
    assert_eq!(
        verus_client::seek_out_of_range(col.iter_range(10, 20), 7, 30..40),
        20..20
    );
    assert_eq!(
        verus_client::seek_out_of_range(col.iter_range(10, 20), 4, 15..12),
        15..15
    );
    assert_eq!(
        verus_client::seek_out_of_range(col.iter_range(10, 20), 4, 2..5),
        10..10
    );
    let mut it = col.iter_range(10, 500);
    assert_eq!(verus_client::find_in_the_rest(&mut it, 7), 21..24);
    assert_eq!((it.pos(), it.next()), (21, Some(7)));
    assert_eq!(verus_client::seek_then_read(&col, 7), 21..24);
    assert_eq!(verus_client::seek_then_read(&col, 4000), 1000..1000);
}

#[test]
fn suspended() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    let mut flags = VerifiedColumn::<bool>::new();
    flags.splice(0, 0, &vec![true, true, false]);

    let it = col.iter_range(5, 50);
    let s = verus_client::told_by_a_suspend(&it, &col);
    let on = verus_client::told_by_a_resume(&s, &col).unwrap();
    assert_eq!((on.pos(), on.end_pos()), (5, 50));
    assert_eq!(on.collect::<Vec<_>>(), v[5..50]);
    verus_client::told_by_the_source(&it, &col);
    let mut on = verus_client::handed_over(col.iter_range(7, 9), &col);
    assert_eq!((on.next(), on.next(), on.next()), (Some(2), Some(2), None));
    assert_eq!(verus_client::stop_and_go_on(&col, 10), (Some(3), Some(3)));
    assert_eq!(verus_client::stop_and_go_on(&col, 11), (Some(3), Some(4)));
    let (s, t) = verus_client::of_a_range(&col, 3, 30);
    assert_eq!(s.try_resume(&col).unwrap().len(), 27);
    assert_eq!(t.try_resume(&col).unwrap().len(), 1000);
    assert!(!verus_client::of_no_column(&col));
    assert!(verus_client::of_no_column(&VerifiedColumn::<u64>::new()));
    for which in 0..10 {
        let mut it = col.iter_range(5, 50);
        verus_client::reads_the_same_column(&mut it, which, 20, 6);
        assert!(it.suspend().try_resume(&col).is_ok(), "{which}");
    }
    let s = verus_client::after_every_move(&col, 3);
    assert!(s.try_resume(&col).is_ok());

    // a state of one column on another that was made the same way
    let (mut a, mut b) = (VerifiedColumn::<u64>::new(), VerifiedColumn::<u64>::new());
    a.splice(0, 0, &vec![9, 8, 7]);
    b.splice(0, 0, &vec![4, 5, 6]);
    let s = a.iter_range(1, 3).suspend();
    assert_eq!(verus_client::resumed_on_any(&s, &b), Some(5));
    assert_eq!(verus_client::resumed_on_any(&s, &col), None);

    let p = verus_client::Paused {
        numbers: col.iter().suspend(),
        flags: flags.iter().suspend(),
    };
    let (x, y, p) = verus_client::step_both(&col, &flags, p).unwrap();
    assert_eq!((x, y), (0, true));
    let (_, _, p) = verus_client::step_both(&col, &flags, p).unwrap();
    let (x, y, p) = verus_client::step_both(&col, &flags, p).unwrap();
    assert_eq!((x, y), (0, false));
    assert!(verus_client::step_both(&col, &flags, p).is_none());
}

#[test]
fn encoders() {
    // each `told_by` client, with numbers
    let (mut a, b) = verus_client::told_by_a_new_encoder::<u64, Leb128>();
    assert_eq!(verus_client::told_by_the_count(&a), (0, true));
    verus_client::told_by_an_append(&mut a, 7);
    verus_client::told_by_an_append_n(&mut a, 7, 2);
    verus_client::told_by_an_owned(&mut a, 9);
    verus_client::told_by_owned_n(&mut a, 5, 0);
    verus_client::told_by_an_extend(&mut a, vec![1, 2, 3]);
    assert_eq!(verus_client::told_by_the_count(&a), (7, false));
    let numbers = verus_client::told_by_an_encoders_save(a);
    assert_eq!(numbers, verus_client::encode_numbers());
    assert_eq!(
        VerifiedColumn::<u64>::load(&numbers).unwrap().to_vec(),
        vec![7, 7, 7, 9, 1, 2, 3]
    );
    let mut out = vec![0xaa, 0xbb];
    assert_eq!(verus_client::told_by_an_encoders_save_to(b, &mut out), 2..2);
    assert_eq!(out, vec![0xaa, 0xbb]);

    // and with byte strings that may be null, and with flags
    let mut s = VerifiedEncoder::<Option<Vec<u8>>>::new();
    verus_client::told_by_an_append(&mut s, Some(&b"ab"[..]));
    verus_client::told_by_an_append_n(&mut s, None, 3);
    verus_client::told_by_an_owned(&mut s, Some(b"c".to_vec()));
    verus_client::told_by_owned_n(&mut s, Some(b"c".to_vec()), 2);
    verus_client::told_by_an_extend(&mut s, vec![Some(&b""[..]), None]);
    assert_eq!(verus_client::told_by_the_count(&s), (9, false));
    let r = verus_client::told_by_an_encoders_save_to(s, &mut out);
    assert_eq!((r.start, r.end), (2, out.len()));
    let strings = VerifiedColumn::<Option<Vec<u8>>>::load(&out[r]).unwrap();
    let c = Some(&b"c"[..]);
    assert_eq!(
        strings.to_vec(),
        vec![Some(&b"ab"[..]), None, None, None, c, c, c, Some(b""), None]
    );
    let mut f = VerifiedEncoder::<bool>::default();
    verus_client::told_by_an_append_n(&mut f, true, 70_000);
    verus_client::told_by_an_owned(&mut f, false);
    verus_client::told_by_owned_n(&mut f, true, 2);
    assert_eq!(verus_client::told_by_the_count(&f), (70_003, false));
    let flags = VerifiedColumn::<bool>::load(&verus_client::told_by_an_encoders_save(f)).unwrap();
    assert_eq!(
        (flags.len(), flags.get(70_000), flags.get(70_002)),
        (70_003, Some(false), Some(true))
    );

    // an encoder and a column agree, whichever way the values come
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    assert_eq!(
        verus_client::encoded_as_saved::<u64, Leb128>(&v),
        col.save()
    );
    assert_eq!(
        verus_client::encoded_as_saved::<u64, Leb128>(&vec![]),
        VerifiedColumn::<u64>::new().save()
    );
    verus_client::encoder_or_column::<u64, Leb128>(&v);
    verus_client::encoder_or_column::<Vec<u8>, Leb128>(&vec![&b"ab"[..], b"", b"ab"]);
    verus_client::encoder_or_column::<Option<i64>, Leb128>(&vec![None, Some(-3), None]);
    verus_client::encoder_or_column::<bool, Leb128>(&vec![false, true, true]);
    assert_eq!(
        verus_client::encode_then_load::<u64, Leb128>(&v).to_vec(),
        v
    );
    let names = vec![Some(&b"ab"[..]), None, Some(b"")];
    assert_eq!(
        verus_client::encode_then_load::<Option<Vec<u8>>, Leb128>(&names).to_vec(),
        names
    );
    assert_eq!(
        verus_client::encode_then_load::<bool, Leb128>(&vec![]).len(),
        0
    );
    assert_eq!(verus_client::reencoded(&col), col.save());
    assert_eq!(verus_client::reencoded(&strings), strings.save());
    assert_eq!(
        verus_client::reencoded(&VerifiedColumn::<bool>::new()),
        VerifiedColumn::<bool>::new().save()
    );

    // run by run
    let runs = vec![
        Run { count: 3, value: 9 },
        Run { count: 0, value: 8 },
        Run {
            count: 70_000,
            value: 9,
        },
    ];
    let by_runs =
        VerifiedColumn::<u64>::load(&verus_client::encoded_by_runs::<u64, Leb128>(&runs)).unwrap();
    assert_eq!(
        (by_runs.len(), by_runs.get(0), by_runs.get(70_002)),
        (70_003, Some(9), Some(9))
    );
    let runs = vec![
        Run {
            count: 2,
            value: &b"x"[..],
        },
        Run {
            count: 1,
            value: b"",
        },
    ];
    let by_runs =
        VerifiedColumn::<Vec<u8>>::load(&verus_client::encoded_by_runs::<Vec<u8>, Leb128>(&runs))
            .unwrap();
    assert_eq!(by_runs.to_vec(), vec![&b"x"[..], b"x", b""]);

    // the worked examples
    let (x, y) = verus_client::encode_strings(b"ab", b"c".to_vec(), Vec::new());
    assert_eq!(
        VerifiedColumn::<Vec<u8>>::load(&x).unwrap().to_vec(),
        vec![&b"ab"[..], b"c", b"ab", b"ab"]
    );
    let y = VerifiedColumn::<Option<Vec<u8>>>::load(&y).unwrap();
    assert_eq!(y.to_vec(), vec![Some(&b"ab"[..]), None, None, Some(b"")]);
    let (mut n, mut f) = (
        VerifiedEncoder::<u64>::new(),
        VerifiedEncoder::<bool>::new(),
    );
    for i in 0..100 {
        verus_client::log_both(&mut n, &mut f, i / 7, i % 3 == 0);
    }
    assert_eq!((n.len(), f.len()), (100, 100));
    assert_eq!(
        VerifiedColumn::<u64>::load(&n.save()).unwrap().get(99),
        Some(14)
    );
    assert_eq!(
        VerifiedColumn::<bool>::load(&f.save()).unwrap().get(99),
        Some(true)
    );

    // a writer of rows, and where it says its columns are
    let mut rows = verus_client::Rows::new();
    for i in 0..300u64 {
        rows.add(i / 5, if i % 2 == 0 { b"even" } else { b"odd" }, i % 4 == 0);
    }
    let mut out = vec![1, 2, 3];
    let w = rows.finish(&mut out);
    assert_eq!(
        (w.ids.start, w.names.start, w.flags.start, w.flags.end),
        (3, w.ids.end, w.names.end, out.len())
    );
    assert_eq!(&out[..3], &[1, 2, 3]);
    let ids = VerifiedColumn::<u64>::load(&out[w.ids]).unwrap();
    let names = VerifiedColumn::<Vec<u8>>::load(&out[w.names]).unwrap();
    let flags = VerifiedColumn::<bool>::load(&out[w.flags]).unwrap();
    assert_eq!((ids.len(), names.len(), flags.len()), (300, 300, 300));
    assert_eq!(
        (ids.get(299), names.get(299), flags.get(296)),
        (Some(59), Some(&b"odd"[..]), Some(true))
    );
    let mut out = vec![9];
    assert_eq!(
        verus_client::rows_written(b"first", b"second", &mut out)
            .unwrap()
            .to_vec(),
        vec![1, 2]
    );
    assert_eq!(out[0], 9);
}

#[test]
fn budgets() {
    // each `told_by` client, with numbers
    let dflt = verus_client::told_by_the_default_budget::<u64, Leb128>();
    assert_eq!((dflt.len(), dflt.save()), (0, vec![]));
    let mut a = verus_client::told_by_a_budget::<u64, Leb128>(4);
    let (b, c) = verus_client::told_by_no_budget::<u64, Leb128>();
    verus_client::told_of_a_budget(&mut a, 7, 8, 9, 3, vec![1, 2]);
    assert_eq!(a.len(), 10);
    let mut out = vec![0xaa];
    let r = verus_client::told_by_an_encoders_save_to_unless(a, &mut out, 7);
    assert_eq!((r.start, r.end), (1, out.len()));
    assert_eq!(
        VerifiedColumn::<u64>::load(&out[r]).unwrap().to_vec(),
        vec![7, 7, 7, 7, 8, 9, 9, 9, 1, 2]
    );
    let held = out.clone();
    let r = verus_client::told_by_an_encoders_save_to_unless(b, &mut out, 7);
    assert_eq!((r, &out), (held.len()..held.len(), &held));
    assert_eq!(verus_client::told_by_an_encoders_column(c).len(), 0);
    let mut d = verus_client::told_by_a_budget::<Option<Vec<u8>>, Leb128>(2);
    verus_client::told_of_a_budget(
        &mut d,
        Some(b"ab"),
        None,
        Some(b"c".to_vec()),
        2,
        vec![None],
    );
    let col = verus_client::told_by_an_encoders_column(d);
    let c2 = Some(&b"c"[..]);
    assert_eq!(
        col.to_vec(),
        vec![
            Some(&b"ab"[..]),
            Some(b"ab"),
            Some(b"ab"),
            None,
            c2,
            c2,
            None
        ]
    );

    // what the range tells
    let mut e = VerifiedEncoder::<bool>::with_max_segments(2);
    e.append_n(true, 5);
    let mut out = vec![1];
    assert_eq!(verus_client::read_from_the_range(e, &mut out, true), 1..1);
    assert_eq!(out, vec![1]);
    let mut e = VerifiedEncoder::<bool>::new();
    e.append(true);
    e.append(false);
    let r = verus_client::read_from_the_range(e, &mut out, true);
    assert_eq!(r.start, 1);
    assert_eq!(
        VerifiedColumn::<bool>::load(&out[r]).unwrap().to_vec(),
        vec![true, false]
    );

    // the worked examples
    let mut out = vec![3];
    verus_client::zeros_take_no_bytes(5, 70_000, &mut out);
    assert_eq!(out, vec![3]);
    for with in [None, Some(2), Some(9)] {
        let mut out = vec![3];
        let r = verus_client::others_take_bytes(with, &mut out);
        assert_eq!(out[0], 3);
        assert_eq!(
            VerifiedColumn::<u64>::load(&out[r]).unwrap().to_vec(),
            vec![0, 5]
        );
    }
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    for with in [None, Some(2), Some(3), Some(16)] {
        assert_eq!(
            verus_client::encoded_into_column::<u64, Leb128>(&v, with).to_vec(),
            v
        );
    }
    let names = vec![Some(&b"ab"[..]), None, Some(b"")];
    assert_eq!(
        verus_client::encoded_into_column::<Option<Vec<u8>>, Leb128>(&names, Some(2)).to_vec(),
        names
    );
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    assert_eq!(
        verus_client::one_column_three_ways::<u64, Leb128>(&v, 2),
        col.save()
    );
    let flags = vec![true, false, false];
    let saved = verus_client::one_column_three_ways::<bool, Leb128>(&flags, 6);
    assert_eq!(
        VerifiedColumn::<bool>::load(&saved).unwrap().to_vec(),
        flags
    );
    let mut out = vec![4];
    for with in [None, Some(2), Some(7)] {
        let r = verus_client::nothing_given_nothing_written::<u64, Leb128>(with, &mut out, 9);
        assert_eq!(r, 1..1);
        let r = verus_client::nothing_given_nothing_written::<Vec<u8>, Leb128>(with, &mut out, b"");
        assert_eq!(r, 1..1);
    }
    assert_eq!(out, vec![4]);
    let (x, y) = verus_client::a_budget_saves_the_same(b"ab", 70_000);
    assert_eq!(x, y);
    let c = VerifiedColumn::<Option<Vec<u8>>>::load(&x).unwrap();
    assert_eq!(
        (c.len(), c.get(0), c.get(70_000)),
        (70_001, Some(Some(&b"ab"[..])), Some(None))
    );
}

#[test]
fn remaps() {
    let numbers = |max: Option<usize>, vs: &[u64]| {
        let mut e = match max {
            Some(max) => VerifiedEncoder::<u64>::with_max_segments(max),
            None => VerifiedEncoder::new(),
        };
        for v in vs {
            e.append(*v);
        }
        e
    };
    let loaded = |bytes: &[u8]| VerifiedColumn::<u64>::load(bytes).unwrap().to_vec();

    // each `told_by` client
    let mut out = vec![0xaa];
    let e = numbers(Some(3), &[1, 1, 2, 3, 3]);
    let r = verus_client::told_by_a_remapping_save(e, &mut out, |v: u64| v + 10);
    assert_eq!((r.start, r.end), (1, out.len()));
    assert_eq!(loaded(&out[r]), vec![11, 11, 12, 13, 13]);
    let held = out.clone();
    let e = numbers(Some(2), &[1, 1, 2, 3, 3]);
    let r = verus_client::told_by_a_remapping_save_unless(e, &mut out, 7, |_: u64| 7);
    assert_eq!((r, &out), (held.len()..held.len(), &held));
    let e = numbers(None, &[1, 1, 2]);
    let r = verus_client::told_by_a_remapping_save_unless(e, &mut out, 7, |v: u64| v / 2 * 7);
    assert_eq!(r.start, held.len());
    assert_eq!(loaded(&out[r]), vec![0, 0, 7]);
    let mut names = VerifiedEncoder::<Option<Vec<u8>>>::new();
    names.append(Some(b"ab"));
    names.append(None);
    names.append_n(Some(b""), 3);
    let mut out = vec![];
    let longer = |v: Option<Vec<u8>>| {
        v.map(|mut s| {
            s.push(b'!');
            s
        })
    };
    let r = verus_client::read_from_the_remapped_range(names, &mut out, None, longer);
    let col = VerifiedColumn::<Option<Vec<u8>>>::load(&out[r]).unwrap();
    let one = Some(&b"!"[..]);
    assert_eq!(col.to_vec(), vec![Some(&b"ab!"[..]), None, one, one, one]);

    // the worked examples
    let mut out = vec![5];
    let r = verus_client::saved_as_it_is(numbers(Some(2), &[4, 4, 9]), &mut out);
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &[4, 4, 9]);
    assert_eq!(out[r], col.save()[..]);

    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    for with in [None, Some(2), Some(16)] {
        let mut out = vec![1, 2];
        let col = verus_client::doubled_as_saved(&v, with, &mut out).unwrap();
        assert_eq!(col.to_vec(), v.iter().map(|x| 2 * x).collect::<Vec<_>>());
        assert_eq!(out[..2], [1, 2]);
    }

    let mut acts = VerifiedEncoder::<Action>::with_max_segments(4);
    acts.append(Action::Make);
    acts.append_n(Action::Increment, 2);
    acts.append(Action::Set);
    let mut out = vec![];
    let r = verus_client::stepped_as_saved(acts, &mut out);
    // an action is saved as its number: Set, Make, Make, Delete
    assert_eq!(loaded(&out[r]), vec![1, 0, 0, 3]);

    let mut flags = VerifiedEncoder::<bool>::with_max_segments(2);
    flags.append_n(true, 3);
    flags.append(false);
    let mut names = VerifiedEncoder::<Vec<u8>>::new();
    names.append(b"a");
    names.append(b"");
    let mut out = vec![9];
    let (p, q) = verus_client::turned_and_longer(flags, names, b'z', &mut out);
    assert_eq!(
        VerifiedColumn::<bool>::load(&out[p]).unwrap().to_vec(),
        vec![false, false, false, true]
    );
    assert_eq!(
        VerifiedColumn::<Vec<u8>>::load(&out[q]).unwrap().to_vec(),
        vec![&b"az"[..], b"z"]
    );

    let mut out = vec![3];
    for with in [None, Some(2), Some(9)] {
        verus_client::zeroed_takes_no_bytes(numbers(with, &[5, 5, 6, 0, 7]), &mut out);
    }
    let mut names = VerifiedEncoder::<Option<Vec<u8>>>::with_max_segments(2);
    names.append(Some(b"ab"));
    names.append(None);
    names.append(Some(b"c"));
    verus_client::nulled_takes_no_bytes(names, &mut out);
    assert_eq!(out, vec![3]);

    let r = verus_client::kept_and_written(numbers(Some(2), &[8, 8, 1]), &mut out, 8);
    assert_eq!(r.start, 1);
    assert_eq!(loaded(&out[r]), vec![8, 8, 1]);
}

#[test]
fn states() {
    // each `told_by` client, with numbers, on a buffer that holds bytes
    let (mut a, b) = verus_client::told_by_a_new_state::<u64, Leb128>();
    let mut buf = vec![0xaa, 0xbb];
    assert_eq!(verus_client::told_by_a_states_count(&a, &buf), (0, true));
    assert!(verus_client::told_by_a_single_run(&a, &buf, 3));
    verus_client::told_by_a_states_append(&mut a, &mut buf, 7);
    verus_client::told_by_a_states_append_n(&mut a, &mut buf, 7, 2);
    assert!(verus_client::told_by_a_single_run(&a, &buf, 7));
    assert!(!verus_client::told_by_a_single_run(&a, &buf, 8));
    verus_client::told_by_a_states_owned(&mut a, &mut buf, 9, 5, 0);
    verus_client::told_by_a_states_extend(&mut a, &mut buf, vec![1, 2, 3]);
    assert_eq!(verus_client::told_by_a_states_count(&a, &buf), (7, false));
    assert!(!verus_client::told_by_a_single_run(&a, &buf, 7));
    verus_client::told_by_a_finish(a, &mut buf);
    assert_eq!(&buf[..2], &[0xaa, 0xbb]);
    assert_eq!(&buf[2..], &verus_client::encode_numbers()[..]);
    let held = buf.clone();
    verus_client::told_by_a_finish(b, &mut buf);
    assert_eq!(buf, held);

    // and with byte strings that may be null, and with flags
    let mut s = VerifiedEncoderState::<Option<Vec<u8>>>::new();
    let at = buf.len();
    verus_client::told_by_a_states_append(&mut s, &mut buf, Some(&b"ab"[..]));
    verus_client::told_by_a_states_append_n(&mut s, &mut buf, None, 3);
    verus_client::told_by_a_states_owned(
        &mut s,
        &mut buf,
        Some(b"c".to_vec()),
        Some(b"c".to_vec()),
        2,
    );
    verus_client::told_by_a_states_extend(&mut s, &mut buf, vec![Some(&b""[..]), None]);
    assert_eq!(verus_client::told_by_a_states_count(&s, &buf), (9, false));
    assert!(!verus_client::told_by_a_single_run(&s, &buf, None));
    verus_client::told_by_a_finish(s, &mut buf);
    assert_eq!(&buf[..at], &held[..]);
    let strings = VerifiedColumn::<Option<Vec<u8>>>::load(&buf[at..]).unwrap();
    let c = Some(&b"c"[..]);
    assert_eq!(
        strings.to_vec(),
        vec![Some(&b"ab"[..]), None, None, None, c, c, c, Some(b""), None]
    );
    let mut f = VerifiedEncoderState::<bool>::default();
    let mut buf = vec![];
    verus_client::told_by_a_states_append_n(&mut f, &mut buf, true, 70_000);
    assert!(verus_client::told_by_a_single_run(&f, &buf, true));
    verus_client::told_by_a_states_owned(&mut f, &mut buf, false, true, 2);
    assert_eq!(
        verus_client::told_by_a_states_count(&f, &buf),
        (70_003, false)
    );
    assert!(!verus_client::told_by_a_single_run(&f, &buf, true));
    verus_client::told_by_a_finish(f, &mut buf);
    let flags = VerifiedColumn::<bool>::load(&buf).unwrap();
    assert_eq!(
        (flags.len(), flags.get(70_000), flags.get(70_002)),
        (70_003, Some(false), Some(true))
    );

    // a state, an encoder, a column and `encode` agree
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    let mut out = vec![9, 9, 9];
    assert_eq!(
        verus_client::streamed_as_saved::<u64, Leb128>(&v, &mut out),
        3..3 + col.save().len()
    );
    assert_eq!((&out[..3], &out[3..]), (&[9, 9, 9][..], &col.save()[..]));
    assert_eq!(
        verus_client::streamed_as_saved::<u64, Leb128>(&vec![], &mut out),
        out.len()..out.len()
    );
    assert_eq!(
        verus_client::one_encoding_four_ways::<u64, Leb128>(v.clone()),
        col.save()
    );
    let names = vec![Some(&b"ab"[..]), None, Some(b"")];
    let saved = verus_client::one_encoding_four_ways::<Option<Vec<u8>>, Leb128>(names.clone());
    assert_eq!(
        VerifiedColumn::<Option<Vec<u8>>>::load(&saved)
            .unwrap()
            .to_vec(),
        names
    );
    assert_eq!(
        verus_client::one_encoding_four_ways::<bool, Leb128>(vec![]),
        Vec::<u8>::new()
    );
    assert_eq!(
        verus_client::one_encoding_four_ways::<Option<i64>, Leb128>(vec![None, Some(-3), None])
            .len(),
        6
    );
    assert_eq!(
        verus_client::stream_then_load::<u64, Leb128>(v.clone()).to_vec(),
        v
    );
    assert_eq!(
        verus_client::stream_then_load::<Option<Vec<u8>>, Leb128>(names.clone()).to_vec(),
        names
    );
    assert_eq!(
        verus_client::stream_then_load::<bool, Leb128>(vec![true, true, false]).to_vec(),
        vec![true, true, false]
    );

    // the worked examples
    let mut out = vec![1];
    let mid = verus_client::strings_streamed(b"ab", b"c".to_vec(), b"".to_vec(), &mut out);
    assert_eq!(out[0], 1);
    let x = VerifiedColumn::<Vec<u8>>::load(&out[1..mid]).unwrap();
    assert_eq!(x.to_vec(), vec![&b"ab"[..], b"c", b"ab", b"ab"]);
    let y = VerifiedColumn::<Option<Vec<u8>>>::load(&out[mid..]).unwrap();
    assert_eq!(y.to_vec(), vec![Some(&b"ab"[..]), None, None, Some(b"")]);
    let mut out = vec![2, 2];
    verus_client::a_single_run(&mut out);
    assert_eq!(
        VerifiedColumn::<u64>::load(&out[2..]).unwrap().to_vec(),
        vec![7, 7, 7, 7, 8]
    );
    let mut log = verus_client::Log::new(vec![5]);
    for x in 0..300 {
        log.add(x / 7);
    }
    let out = log.finish();
    assert_eq!(out[0], 5);
    let back = VerifiedColumn::<u64>::load(&out[1..]).unwrap().to_vec();
    assert_eq!(back, (0..300).map(|x| x / 7).collect::<Vec<u64>>());
    assert_eq!(
        verus_client::logged(vec![1, 2, 3]).unwrap().to_vec(),
        vec![4, 5, 6]
    );
    let mut out = vec![6, 6];
    verus_client::given_up::<u64, Leb128>(v.clone(), &mut out);
    assert_eq!(out, vec![6, 6]);
    verus_client::given_up::<Option<Vec<u8>>, Leb128>(names.clone(), &mut out);
    assert_eq!(out, vec![6, 6]);
    assert!(verus_client::written_unless_one::<u64, Leb128>(
        vec![4; 500],
        4,
        &mut out
    ));
    assert!(verus_client::written_unless_one::<u64, Leb128>(
        vec![],
        4,
        &mut out
    ));
    assert!(verus_client::written_unless_one::<bool, Leb128>(
        vec![false; 9],
        false,
        &mut out
    ));
    assert_eq!(out, vec![6, 6]);
    assert!(!verus_client::written_unless_one::<u64, Leb128>(
        v.clone(),
        4,
        &mut out
    ));
    assert_eq!((&out[..2], &out[2..]), (&[6, 6][..], &col.save()[..]));
}

#[test]
fn statics() {
    // each `told_by` client
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    assert_eq!(
        verus_client::told_by_an_encode::<u64, Leb128>(v.clone()),
        col.save()
    );
    assert_eq!(
        verus_client::told_by_an_encode::<bool, Leb128>(vec![]),
        Vec::<u8>::new()
    );
    let mut out = vec![7];
    let r = verus_client::told_by_an_encode_to::<u64, Leb128>(&mut out, v.clone());
    assert_eq!((r.start, r.end, &out[r]), (1, out.len(), &col.save()[..]));
    let names = vec![Some(&b"ab"[..]), None, Some(b"")];
    let r = verus_client::told_by_an_encode_to::<Option<Vec<u8>>, Leb128>(&mut out, names.clone());
    assert_eq!(
        VerifiedColumn::<Option<Vec<u8>>>::load(&out[r])
            .unwrap()
            .to_vec(),
        names
    );
    let held = out.clone();
    let r = verus_client::told_by_an_encode_to_unless::<u64, Leb128>(&mut out, vec![3; 40], 3);
    assert_eq!((r, &out), (held.len()..held.len(), &held));
    let r = verus_client::told_by_an_encode_to_unless::<Option<Vec<u8>>, Leb128>(
        &mut out,
        vec![None; 4],
        None,
    );
    assert_eq!((r, &out), (held.len()..held.len(), &held));
    let r = verus_client::told_by_an_encode_to_unless::<bool, Leb128>(&mut out, vec![], true);
    assert_eq!((r, &out), (held.len()..held.len(), &held));
    let r = verus_client::told_by_an_encode_to_unless::<bool, Leb128>(
        &mut out,
        vec![true, false],
        true,
    );
    assert_eq!((r.start, r.end), (held.len(), out.len()));
    assert_eq!(
        VerifiedColumn::<bool>::load(&out[r]).unwrap().to_vec(),
        vec![true, false]
    );

    // the worked examples
    let mut out = vec![8, 8];
    let flags: Vec<bool> = (0..77).map(|i| i % 5 < 2).collect();
    let (x, y) = verus_client::two_columns_one_buffer(v.clone(), flags.clone(), &mut out);
    assert_eq!(
        (&out[..2], x.start, x.end, y.end),
        (&[8, 8][..], 2, y.start, out.len())
    );
    assert_eq!(VerifiedColumn::<u64>::load(&out[x]).unwrap().to_vec(), v);
    assert_eq!(
        VerifiedColumn::<bool>::load(&out[y]).unwrap().to_vec(),
        flags
    );
    let mut out = vec![3];
    verus_client::all_the_one_leave_the_buffer::<u64, Leb128>(vec![0; 70_000], 0, &mut out);
    verus_client::all_the_one_leave_the_buffer::<Vec<u8>, Leb128>(vec![b"ab"; 5], b"ab", &mut out);
    verus_client::nulls_leave_the_buffer(&mut out);
    assert_eq!(out, vec![3]);
    let r = verus_client::another_is_written(&mut out);
    assert_eq!(out[0], 3);
    assert_eq!(
        VerifiedColumn::<u64>::load(&out[r]).unwrap().to_vec(),
        vec![0, 5]
    );
    let mut out = vec![1];
    assert_eq!(
        verus_client::read_from_the_encoded_range::<bool, Leb128>(vec![true; 5], &mut out, true),
        1..1
    );
    assert_eq!(out, vec![1]);
    let r = verus_client::read_from_the_encoded_range::<bool, Leb128>(
        vec![true, false],
        &mut out,
        true,
    );
    assert_eq!(r.start, 1);
    assert_eq!(
        VerifiedColumn::<bool>::load(&out[r]).unwrap().to_vec(),
        vec![true, false]
    );
}

#[test]
fn strings() {
    let words = vec!["pear", "", "apple", "apple", "größer", "日本語", "🦀"];
    verus_client::round_trip_text(&words);
    verus_client::text_in_text_out("größer", "");
    let mut col = VerifiedColumn::<String>::new();
    col.splice(0, 0, &words);
    assert!(verus_client::the_string_itself(&mut col, "日本語"));
    assert_eq!(col.len(), 8);

    let bytes = verus_client::fruit();
    let back = VerifiedColumn::<String>::load(&bytes).unwrap();
    assert_eq!(back.to_vec(), vec!["apple", "pear", "", "pear"]);
    let long = "x".repeat(1001);
    assert_eq!(verus_client::saved_if_short(&long), None);
    let short = verus_client::saved_if_short(&long[..1000]).unwrap();
    let back = VerifiedColumn::<String>::load(&short).unwrap();
    assert_eq!(back.to_vec(), vec![&long[..1000]]);

    // the order is that of the bytes, which is `str`'s own
    verus_client::find_text("apple", "pear");
    verus_client::find_text("", "\0");
    verus_client::find_text("z", "é");
    verus_client::find_text("pear", "pears");
    verus_client::find_fruit();
    for a in &words {
        for b in &words {
            assert_eq!(verus_client::text_compared(a, b), a.cmp(b));
            let later = verus_client::later_of::<String, Leb128>(a, b);
            assert_eq!(later, *std::cmp::max(a, b));
        }
    }
    let mut sorted = VerifiedColumn::<String>::new();
    for w in &words {
        verus_client::insert_in_order(&mut sorted, w);
    }
    let mut expect = words.clone();
    expect.sort();
    assert_eq!(sorted.to_vec(), expect);

    verus_client::exclaimed(&mut col);
    assert_eq!(
        col.to_vec(),
        vec![
            "pear!",
            "!",
            "apple!",
            "apple!",
            "größer!",
            "日本語!",
            "🦀!",
            "日本語!"
        ]
    );
    verus_client::remap_as_it_is(&mut col);
    assert_eq!(verus_client::read_in_a_loop(&col).len(), 8);
    let runs = verus_client::in_runs(&col);
    assert_eq!(runs.len(), 7);
    assert_eq!(
        runs[2],
        Run {
            count: 2,
            value: "apple!"
        }
    );

    let bytes = verus_client::encode_text("ab", "çd".to_string());
    let back = VerifiedColumn::<String>::load(&bytes).unwrap();
    assert_eq!(back.to_vec(), vec!["ab", "çd", "ab", "ab"]);
    let mut buf = vec![9u8];
    verus_client::text_streamed("ab", "çd".to_string(), &mut buf);
    assert_eq!(buf[0], 9);
    assert_eq!(&buf[1..], &bytes[..]);
    verus_client::load_text(&bytes);
    // one string of one byte that is not UTF-8: it does not load
    verus_client::load_text(&[0x7f, 1, 0xff]);
    assert!(VerifiedColumn::<String>::load(&[0x7f, 1, 0xff]).is_err());
    assert!(VerifiedColumn::<Vec<u8>>::load(&[0x7f, 1, 0xff]).is_ok());

    // the clients that are for every value type, of strings
    verus_client::round_trip::<String, Leb128, LenIndex>(&words);
    verus_client::two_in_one_buffer::<String, Leb128>(&words, &vec![]);
    verus_client::one_value_only::<String, Leb128>("a", "");
    verus_client::clone_then_diverge::<String, Leb128, LenIndex>(&words, "c");
    verus_client::collect_then_extend::<String, Leb128, LenIndex>(
        vec!["a", "a"],
        vec![],
        vec!["é"],
    );
    verus_client::runs_in::<String, Leb128, LenIndex>(
        "a",
        "b",
        "a",
        vec![Run {
            count: 2,
            value: "",
        }],
    );
    verus_client::load_with_a_length::<String, Leb128>(&words, "x", 3);
    for n in [0, 1, 100, 70_000, usize::MAX - 3, usize::MAX] {
        verus_client::load_a_fill::<String, Leb128>("ab", n);
    }
    verus_client::slabs_and_validation::<String, Leb128, LenIndex>(&words);
    verus_client::load_then_validate::<String, Leb128, LenIndex>(&bytes);
    verus_client::runs_in_a_window::<String, Leb128, LenIndex>("", "é");
    assert_eq!(verus_client::the_third(&col), Some("apple!"));
    let mut enc = VerifiedEncoder::<String>::new();
    verus_client::told_by_an_append(&mut enc, "ab");
    verus_client::told_by_an_owned(&mut enc, "çd".to_string());
    verus_client::told_by_an_append_n(&mut enc, "ab", 2);
    assert_eq!(enc.save(), bytes);
}

#[test]
fn edits() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    assert!(!verus_client::ended_by_a_push(&mut col, 7));
    assert_eq!(col.len(), 1001);
    let mut none = VerifiedColumn::<Option<Vec<u8>>>::new();
    assert!(!verus_client::ended_by_a_push(&mut none, None));

    for which in 0..12 {
        let mut col = VerifiedColumn::<u64>::new();
        col.splice(0, 0, &v);
        let runs = vec![Run { count: 2, value: 0 }];
        let ended = !verus_client::ended_by_each_edit(&mut col, 0, vec![0], runs, which);
        assert!(ended, "{which}");
        // a column of byte strings, in slabs of few values
        let mut col = VerifiedColumn::<Vec<u8>>::with_max_segments(4);
        col.splice(0, 0, &vec![&b"ab"[..]; 40]);
        let runs = vec![Run {
            count: 2,
            value: &b"ab"[..],
        }];
        let ended = !verus_client::ended_by_each_edit(&mut col, b"ab", vec![b"ab"], runs, which);
        assert!(ended, "{which}");
    }

    let s = col.iter_range(3, 9).suspend();
    verus_client::no_edits(&mut col, 5, &[]);
    assert_eq!(s.try_resume(&col).unwrap().len(), 6);
    let mut empty = VerifiedColumn::<bool>::new();
    verus_client::cleared(&mut empty);
    verus_client::cleared(&mut col);
    assert!(s.try_resume(&col).is_err());
    assert!(col.is_empty());

    col.splice(0, 0, &v);
    assert!(!verus_client::ended_by_many(&mut col, 1, 1));
    assert!(!verus_client::ended_by_many(&mut col, 1, 300));
    assert!(!verus_client::ended_though_undone(&mut col, 1));
    assert_eq!(col.len(), 1301);
    assert_eq!(verus_client::a_copy_edited(&col, 4), (false, true));

    // what each call says of the count
    let s = col.iter().suspend();
    verus_client::counted_by_a_splice(&mut col, 3, 0, &[]);
    verus_client::counted_by_a_splice_iter(&mut col, 3, 0, None);
    verus_client::counted_by_a_splice_runs(&mut col, 3, 0, vec![Run { count: 0, value: 9 }]);
    verus_client::counted_by_an_extend(&mut col, Vec::new());
    verus_client::counted_by_a_truncate(&mut col, 1301);
    verus_client::counted_by_a_remove_n(&mut col, 1301, 0);
    let copy = verus_client::counted_by_a_clone(&col);
    assert!(s.try_resume(&col).is_ok() && s.try_resume(&copy).is_ok());
    for which in 0..10 {
        let mut col = copy.clone();
        match which {
            0 => verus_client::counted_by_a_splice(&mut col, 3, 1, &[]),
            1 => verus_client::counted_by_a_splice_iter(&mut col, 3, 0, Some(1)),
            2 => verus_client::counted_by_a_splice_runs(
                &mut col,
                3,
                0,
                vec![Run { count: 1, value: 9 }],
            ),
            3 => verus_client::counted_by_an_extend(&mut col, vec![1]),
            4 => verus_client::counted_by_an_insert(&mut col, 0, 1),
            5 => verus_client::counted_by_a_push(&mut col, 1),
            6 => verus_client::counted_by_a_remove(&mut col, 0),
            7 => verus_client::counted_by_a_truncate(&mut col, 1300),
            8 => verus_client::counted_by_a_remove_n(&mut col, 1300, 1),
            _ => verus_client::counted_by_a_remap(&mut col, |x: u64| x),
        }
        assert!(s.try_resume(&col).is_err(), "{which}");
        assert!(s.try_resume(&copy).is_ok(), "{which}");
    }
    let mut col = copy.clone();
    verus_client::counted_by_a_clear(&mut col);
    assert!(s.try_resume(&col).is_err());
}

#[test]
fn decoders() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, &v);
    let bytes = col.save();
    let all = verus_client::read_all(VerifiedDecoder::<u64>::new(&bytes));
    assert_eq!(all, v);
    assert_eq!(verus_client::decode_all::<u64, Leb128>(&bytes), v);
    assert!(verus_client::decoded_as_held(&col));
    assert!(verus_client::encoded_then_decoded::<u64, Leb128>(&v));
    let known = verus_client::decode_unknown::<u64, Leb128>(&bytes);
    assert_eq!(known, Some(v.clone()));
    // a run of two with no value after it
    assert_eq!(verus_client::decode_unknown::<u64, Leb128>(&[2]), None);
    assert!(verus_client::decoded_or_loaded::<u64, Leb128>(&bytes));
    assert!(verus_client::decoded_or_loaded::<u64, Leb128>(&[2]));
    assert!(verus_client::decoded_or_loaded::<bool, Leb128>(&[3, 2]));
    assert_eq!(verus_client::the_sixth(&bytes), Some(1));
    assert_eq!(verus_client::the_sixth(&[]), None);
    assert_eq!(verus_client::recoded_by_runs::<u64, Leb128>(&bytes), bytes);
    let mut dec = VerifiedDecoder::<u64>::new(&bytes);
    for _ in 0..200 {
        assert!(verus_client::runs_differ(&mut dec));
    }
    assert_eq!(dec.next(), None);
    verus_client::a_run_in_parts();
    verus_client::nulls_and_flags_decoded();
    verus_client::after_a_scan();

    // strings, and byte strings that may be null
    let words = vec!["", "apple", "apple", "größer", "pear", "日本語"];
    let mut text = VerifiedColumn::<String>::new();
    text.splice(0, 0, &words);
    let text_bytes = text.save();
    assert_eq!(verus_client::first_text(&text_bytes), Some(""));
    assert_eq!(verus_client::first_text(&[]), None);
    let all = verus_client::decode_all::<String, Leb128>(&text_bytes);
    assert_eq!(all, words);
    assert!(verus_client::decoded_as_held(&text));
    assert!(verus_client::encoded_then_decoded::<String, Leb128>(&words));
    let again = verus_client::recoded_by_runs::<String, Leb128>(&text_bytes);
    assert_eq!(again, text_bytes);
    for x in ["", "apple", "b", "pear", "日本語", "🦀"] {
        assert!(verus_client::scanned_as_searched(&text, x), "{x}");
    }
    let blobs = vec![None, None, Some(&b"ab"[..]), Some(b""), None];
    let mut blob = VerifiedColumn::<Option<Vec<u8>>>::new();
    blob.splice(0, 0, &blobs);
    let blob_bytes = blob.save();
    assert_eq!(verus_client::first_bytes(&blob_bytes), Some(None));
    let mut some = VerifiedColumn::<Option<Vec<u8>>>::new();
    some.push(Some(b"ab"));
    let some_bytes = some.save();
    let first = verus_client::first_bytes(&some_bytes);
    assert_eq!(first, Some(Some(&b"ab"[..])));
    assert_eq!(verus_client::first_bytes(&[]), None);
    assert!(verus_client::decoded_as_held(&blob));
    assert!(verus_client::encoded_then_decoded::<Option<Vec<u8>>, Leb128>(&blobs));
    let again = verus_client::recoded_by_runs::<Option<Vec<u8>>, Leb128>(&blob_bytes);
    assert_eq!(again, blob_bytes);

    // a scan of numbers in order
    let sorted: Vec<u32> = (0..1000).map(|i| i / 3 * 2).collect();
    let mut scol = VerifiedColumn::<u32>::new();
    scol.splice(0, 0, &sorted);
    let sbytes = scol.save();
    assert_eq!(verus_client::find_decoded(&sbytes, 0), (0, 3));
    assert_eq!(verus_client::find_decoded(&sbytes, 10), (15, 3));
    assert_eq!(verus_client::find_decoded(&sbytes, 11), (18, 0));
    assert_eq!(verus_client::find_decoded(&sbytes, 666), (999, 1));
    assert_eq!(verus_client::find_decoded(&sbytes, 7000), (1000, 0));
    for x in [0, 1, 10, 11, 666, 667, 7000] {
        assert!(verus_client::scanned_as_searched(&scol, x), "{x}");
    }

    // what each call says
    let mut dec = verus_client::told_by_a_decoder::<u64, Leb128>(&bytes);
    assert!(verus_client::told_by_a_try::<u64, Leb128>(&bytes).is_ok());
    assert!(verus_client::told_by_a_try::<u64, Leb128>(&[2]).is_err());
    assert_eq!(verus_client::told_by_a_decoded_value(&mut dec), Some(0));
    assert_eq!(verus_client::told_by_a_decoders_nth(&mut dec, 4), Some(1));
    verus_client::told_by_an_advance(&mut dec, 3);
    let run = verus_client::told_by_a_decoded_run(&mut dec);
    assert_eq!(run, Some(Run { count: 3, value: 3 }));
    let run = verus_client::told_by_a_bounded_run(&mut dec, 2);
    assert_eq!(run, Some(Run { count: 2, value: 4 }));
    assert_eq!(verus_client::told_by_a_bounded_run(&mut dec, 0), None);
    let found = verus_client::told_by_a_decoders_scan(&mut dec, 6, 100);
    assert_eq!(found, (4, 3));
    let copy = verus_client::told_by_a_decoders_copy(&dec);
    assert_eq!(copy.collect::<Vec<_>>(), v[21..]);
    assert_eq!(dec.next(), Some(7));
    verus_client::told_by_an_advance(&mut dec, 2000);
    assert_eq!(verus_client::told_by_a_decoded_value(&mut dec), None);
    assert_eq!(verus_client::told_by_a_decoders_nth(&mut dec, 0), None);
    assert_eq!(verus_client::told_by_a_decoded_run(&mut dec), None);
    let found = verus_client::told_by_a_decoders_scan(&mut dec, 6, 100);
    assert_eq!(found, (0, 0));
    let whole = VerifiedDecoder::<u64>::new(&bytes);
    assert_eq!(verus_client::told_by_a_decoders_loop(whole), Some(333));
    assert_eq!(verus_client::told_by_a_decoders_loop(dec), None);
}

#[test]
fn sums() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let plain = VerifiedColumn::<u64>::from_values(&v);
    let bytes = plain.save();
    // the clients of a column of any kind, of prefix columns
    verus_client::round_trip::<u32, Leb128, SumIndex>(&vec![1, 2, 2, u32::MAX]);
    verus_client::round_trip::<Option<i64>, Leb128, SumIndex>(&vec![
        Some(i64::MIN),
        None,
        Some(-1),
    ]);
    verus_client::round_trip::<bool, Leb128, SumIndex>(&vec![true, false, false]);
    verus_client::load_then_save::<u64, Leb128, SumIndex>(&bytes);
    verus_client::clone_then_diverge::<u64, Leb128, SumIndex>(&v, 7);
    verus_client::collect_then_extend::<u64, Leb128, SumIndex>(vec![1, 1, 2], vec![3], vec![4, 4]);
    verus_client::slabs_and_validation::<u64, Leb128, SumIndex>(&(0..10_000).collect());
    verus_client::runs_in_a_window::<u64, Leb128, SumIndex>(1, 2);
    let col: VerifiedPrefixColumn<u64> = verus_client::copied_by_runs(&plain);
    assert_eq!(col.to_vec(), v);
    assert_eq!(verus_client::read_in_a_loop(&col), v);
    assert_eq!(verus_client::in_runs(&col).len(), 334);
    assert_eq!(verus_client::count_by_runs(&col, 10, 500), 490);
    let back: VerifiedColumn<u64> = verus_client::copied_by_runs(&col);
    assert_eq!(back.to_vec(), v);
    // loads
    let loaded = verus_client::load_with_sums::<u64, Leb128>(&bytes).unwrap();
    assert_eq!(loaded.to_vec(), v);
    assert!(verus_client::load_with_sums::<u64, Leb128>(&[2]).is_none());
    assert!(verus_client::load_short_sums::<u64, Leb128>(&bytes).is_some());
    assert!(verus_client::load_short_sums::<bool, Leb128>(&[3, 2]).is_some());
    let wide = vec![Some(i64::MIN), None, Some(i64::MAX)];
    let signed = VerifiedColumn::<Option<i64>>::from_values(&wide).save();
    let (a, b) = verus_client::load_wide_sums(&bytes, &signed);
    assert_eq!(a.unwrap().to_vec(), v);
    assert_eq!(b.unwrap().to_vec(), wide);
    let small = VerifiedColumn::<u32>::from_values(&[u32::MAX, 0, 7]).save();
    assert_eq!(
        verus_client::load_small_sums(&small).unwrap().to_vec(),
        vec![u32::MAX, 0, 7]
    );
    // edits
    let mut col = loaded;
    verus_client::push_within_the_sums(&mut col, u64::MAX);
    assert_eq!((col.len(), col.get(1000)), (1001, Some(u64::MAX)));
    let mut flags = VerifiedPrefixColumn::<bool>::new();
    verus_client::push_within_the_sums(&mut flags, true);
    assert_eq!(flags.to_vec(), vec![true]);
    // saved as a plain column of the values
    let sums = verus_client::summed_up(&plain);
    let (x, y) = verus_client::saved_alike(&plain, &sums);
    assert_eq!(x, y);
    assert_eq!(x, bytes);
}

#[cfg(target_pointer_width = "64")]
#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_fill_past_the_sums() {
    verus_client::a_fill_past_the_sums(1 << 33);
}

#[cfg(target_pointer_width = "64")]
#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_push_past_the_sums() {
    let mut col = VerifiedPrefixColumn::<u32>::fill((1 << 32) + 1, u32::MAX);
    verus_client::a_push_past_the_sums(&mut col, 1);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn sums_refused() {
    // more than a `u32` column's sums hold: no prefix column, a plain one
    let long = VerifiedColumn::<u32>::fill(1 << 33, u32::MAX).save();
    assert!(verus_client::load_with_sums::<u32, Leb128>(&long).is_none());
    assert!(verus_client::load_plain::<u32, Leb128>(&long).is_some());
    // the most they hold
    let most = VerifiedColumn::<u32>::fill((1 << 32) + 1, u32::MAX).save();
    let mut col = verus_client::load_small_sums(&most).unwrap();
    verus_client::push_within_the_sums(&mut col, 0);
    assert_eq!(col.len(), (1 << 32) + 2);
}

#[cfg(target_pointer_width = "64")]
#[test]
#[should_panic(expected = "prefix sum overflows")]
fn sums_of_a_plain_column_past_the_type() {
    verus_client::summed_up(&VerifiedColumn::<u32>::fill(1 << 33, u32::MAX));
}

#[test]
fn sums_read() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let before = |i: usize| v[..i].iter().map(|x| u128::from(*x)).sum::<u128>();
    let all = before(1000);
    for i in [0, 1, 500, 999, 1000] {
        assert_eq!(verus_client::the_sum_before(&col, i), before(i));
    }
    for i in [0, 1, 500, 999] {
        let (a, b) = verus_client::the_sum_through(&col, i);
        assert_eq!((a, b), (before(i), before(i + 1)));
    }
    for (start, end) in [(0, 0), (0, 1000), (3, 700), (999, 1000), (1000, 1000)] {
        let (a, b, range) = verus_client::the_sum_of_a_range(&col, start, end);
        assert_eq!(
            (a, b, range),
            (before(start), before(end), before(end) - before(start))
        );
    }
    assert_eq!(verus_client::sums_past_the_end(&col), (all, all, all, 0));
    let none = VerifiedPrefixColumn::<Option<i64>>::new();
    assert_eq!(verus_client::sums_past_the_end(&none), (0, 0, 0, 0));
    // signed values
    let signed = VerifiedPrefixColumn::<i64>::from_values(&[4, -9, i64::MIN, 2]);
    assert_eq!(verus_client::the_sum_before(&signed, 2), -5);
    let (a, b, range) = verus_client::the_sum_of_a_range(&signed, 1, 3);
    assert_eq!(
        (a, b, range),
        (4, -5 + i128::from(i64::MIN), -9 + i128::from(i64::MIN))
    );
    assert_eq!(
        verus_client::a_range_by_its_ends(&signed, 1, 3),
        -9 + i128::from(i64::MIN)
    );
    assert_eq!(verus_client::a_range_by_its_ends(&signed, 2, 2), 0);
    let wide = VerifiedPrefixColumn::<i64>::from_values(&[i64::MIN, i64::MAX, i64::MAX]);
    assert_eq!(
        verus_client::a_range_by_its_ends(&wide, 0, 1),
        i128::from(i64::MIN)
    );
    assert_eq!(
        verus_client::a_range_by_its_ends(&wide, 1, 3),
        2 * i128::from(i64::MAX)
    );
    // after an edit
    let mut col = col;
    assert_eq!(
        verus_client::a_sum_after_a_push(&mut col, 77),
        (all, all + 77)
    );
    assert_eq!(col.len(), 1001);
    let mut flags = VerifiedPrefixColumn::<bool>::from_values(&[true, false, true]);
    assert_eq!(verus_client::a_sum_after_a_push(&mut flags, true), (2, 3));
    assert_eq!(verus_client::a_sum_after_a_push(&mut flags, false), (3, 3));
}

#[test]
fn sums_searched() {
    let v: Vec<u64> = vec![3, 0, 4, 4, 10];
    let col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let places: Vec<Option<usize>> = [0, 1, 3, 4, 7, 8, 21, 22]
        .into_iter()
        .map(|t| verus_client::the_place_of_a_sum(&col, t))
        .collect();
    assert_eq!(
        places,
        [
            Some(0),
            Some(1),
            Some(1),
            Some(3),
            Some(3),
            Some(4),
            Some(5),
            None
        ]
    );
    assert_eq!(verus_client::a_sum_and_its_place(&col, 0), None);
    assert_eq!(verus_client::a_sum_and_its_place(&col, 3), Some((0, 3)));
    assert_eq!(verus_client::a_sum_and_its_place(&col, 5), Some((3, 7)));
    assert_eq!(verus_client::a_sum_and_its_place(&col, 21), Some((11, 21)));
    assert_eq!(verus_client::a_sum_and_its_place(&col, 22), None);
    let small = VerifiedPrefixColumn::<Option<u32>>::from_values(&[None, Some(u32::MAX), Some(1)]);
    assert_eq!(verus_client::the_place_of_a_sum(&small, 1), Some(2));
    assert_eq!(verus_client::the_place_of_a_sum(&small, 1 << 32), Some(3));
    assert_eq!(
        verus_client::the_place_of_a_sum(&small, (1 << 32) + 1),
        None
    );
    let none = VerifiedPrefixColumn::<u32>::new();
    assert_eq!(verus_client::the_place_of_a_sum(&none, 0), Some(0));
    assert_eq!(verus_client::the_place_of_a_sum(&none, 1), None);
    assert_eq!(verus_client::the_two_places(&none, 0), (0, 0));
    assert_eq!(verus_client::the_two_places(&none, 1), (1, 0));
    assert_eq!(verus_client::the_two_places(&col, 0), (0, 0));
    assert_eq!(verus_client::the_two_places(&col, 4), (3, 2));
    assert_eq!(verus_client::the_two_places(&col, 22), (6, 5));
    // the flags that are set, in order
    let flags: Vec<bool> = (0..3000).map(|i| i % 7 == 2 || i % 11 == 0).collect();
    let col = VerifiedPrefixColumn::<bool>::from_values(&flags);
    let set: Vec<usize> = (0..flags.len()).filter(|i| flags[*i]).collect();
    for (k, place) in set.iter().enumerate() {
        assert_eq!(verus_client::the_kth_true(&col, k), Some(*place));
    }
    assert_eq!(verus_client::the_kth_true(&col, set.len()), None);
    assert_eq!(verus_client::the_kth_true(&col, usize::MAX - 1), None);
}

#[test]
fn sums_read_in_step() {
    let v: Vec<u64> = (0..1000).map(|i| i / 3).collect();
    let col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let before = |i: usize| v[..i].iter().map(|x| u128::from(*x)).sum::<u128>();
    // each running sum, of the column and of a range
    let sums = verus_client::running_sums(&col);
    assert_eq!(sums, (1..=1000).map(before).collect::<Vec<_>>());
    assert!(verus_client::running_sums(&VerifiedPrefixColumn::<Option<i64>>::new()).is_empty());
    for (a, b) in [(0, 0), (0, 1000), (3, 700), (999, 1000), (1000, 1000)] {
        let want: Vec<(u64, u128)> = (a..b).map(|i| (v[i], before(i))).collect();
        assert_eq!(verus_client::sums_in_a_range(&col, a, b), want);
        let range = before(b) - before(a);
        assert_eq!(verus_client::summed_by_moving(&col, a, b), (range, range));
    }
    // one value with its sums
    for i in [0, 1, 500, 999] {
        let sums = (before(i), before(i + 1));
        assert_eq!(
            verus_client::a_value_with_its_sums(&col, i),
            (sums.0, v[i], sums.1)
        );
        assert_eq!(verus_client::sums_two_ways(&col, i), (sums, sums));
        let pv = verus_client::told_by_a_get_prefixed(&col, i).unwrap();
        assert_eq!((pv.prefix(), pv.value, pv.total()), (sums.0, v[i], sums.1));
        assert_eq!(
            verus_client::told_by_a_prefixed_value(&pv),
            (sums.1, sums.0)
        );
    }
    assert!(verus_client::told_by_a_get_prefixed(&col, 1000).is_none());
    for (i, j) in [(0, 1), (0, 999), (17, 400), (998, 999)] {
        assert_eq!(
            verus_client::two_skips(&col, i, j),
            (before(i + 1), before(j + 1))
        );
    }
    // step by step, to the end
    let mut it = verus_client::told_by_a_prefix_iter_range(&col, 998, 1000);
    assert_eq!(
        verus_client::a_step_with_its_sums(&mut it),
        Some((before(998), v[998], before(999)))
    );
    assert_eq!(
        verus_client::a_step_with_its_sums(&mut it),
        Some((before(999), v[999], before(1000)))
    );
    assert_eq!(verus_client::a_step_with_its_sums(&mut it), None);
    // the first sum past a limit
    for limit in [0, 1, 2, 3, 1000, before(1000) - 1, before(1000), u128::MAX] {
        let first = (0..1000).find(|i| before(i + 1) > limit);
        assert_eq!(verus_client::first_over(&col, limit), first);
    }
    // signed values, nulls and flags
    let signed = VerifiedPrefixColumn::<i64>::from_values(&[4, -9, i64::MIN, 2]);
    let least = i128::from(i64::MIN);
    assert_eq!(
        verus_client::running_sums(&signed),
        [4, -5, -5 + least, -3 + least]
    );
    assert_eq!(
        verus_client::sums_in_a_range(&signed, 1, 4),
        [(-9, 4), (i64::MIN, -5), (2, -5 + least)]
    );
    assert_eq!(
        verus_client::summed_by_moving(&signed, 1, 3),
        (-9 + least, -9 + least)
    );
    let small = VerifiedPrefixColumn::<Option<u32>>::from_values(&[None, Some(u32::MAX), Some(1)]);
    assert_eq!(
        verus_client::running_sums(&small),
        [0, u64::from(u32::MAX), 1 << 32]
    );
    assert_eq!(
        verus_client::a_value_with_its_sums(&small, 2),
        (u64::from(u32::MAX), Some(1), 1 << 32)
    );
    let flags = VerifiedPrefixColumn::<bool>::from_values(&[true, false, true]);
    assert_eq!(verus_client::running_sums(&flags), [1, 1, 2]);
    assert_eq!(verus_client::two_skips(&flags, 0, 2), (1, 2));
}

#[test]
fn sums_kept_by_each_move() {
    let v: Vec<u64> = (0..1000).map(|i| i % 13).collect();
    let col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let before = |i: usize| v[..i].iter().map(|x| u128::from(*x)).sum::<u128>();
    verus_client::moved_with_sums(&VerifiedPrefixColumn::<u64>::from_values(&v[..10]));

    let mut it = verus_client::told_by_a_prefix_iter(&col);
    assert_eq!(verus_client::told_of_the_running_sum(&it), 0);
    assert_eq!(verus_client::told_of_the_sum_before(&it), 0);
    assert_eq!(verus_client::told_by_a_prefix_iters_place(&it), 0);
    assert_eq!(verus_client::told_by_a_prefix_iters_end(&it), 1000);
    assert_eq!(
        verus_client::told_by_a_prefix_iters_count(&it),
        (1000, 1000)
    );
    assert_eq!(
        verus_client::told_by_a_prefix_size_hint(&it),
        (1000, Some(1000))
    );
    verus_client::told_of_what_is_left(&it);
    // read, and stepped over
    let pv = verus_client::told_by_a_prefixed_next(&mut it).unwrap();
    assert_eq!((pv.prefix(), pv.value, pv.total()), (0, v[0], before(1)));
    let pv = verus_client::told_by_a_prefixed_nth(&mut it, 499).unwrap();
    assert_eq!(
        (pv.prefix(), pv.value, pv.total()),
        (before(500), v[500], before(501))
    );
    assert_eq!(verus_client::told_of_the_running_sum(&it), before(501));
    assert_eq!(verus_client::told_of_the_sum_before(&it), before(501));
    // the window's end moved, and the iterator
    verus_client::told_by_a_prefix_set_max(&mut it, 900);
    assert_eq!(verus_client::told_by_a_prefix_iters_count(&it), (399, 399));
    assert_eq!(
        verus_client::told_by_a_prefix_size_hint(&it),
        (399, Some(399))
    );
    verus_client::told_by_a_prefix_advance_to(&mut it, 600);
    assert_eq!((it.pos(), it.total()), (600, before(600)));
    verus_client::told_by_a_prefix_advance_by(&mut it, 50);
    assert_eq!((it.pos(), it.total()), (650, before(650)));
    // its sum started again
    verus_client::told_by_a_reset(&mut it);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (650, 900, 0));
    let since = |i: usize| before(i) - before(650);
    verus_client::told_by_a_prefix_shift(&mut it, 700..950);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (700, 950, since(700)));
    // a window that ends past the column
    let pv = verus_client::told_by_a_prefix_shift_next(&mut it, 800..2000).unwrap();
    assert_eq!(
        (pv.prefix(), pv.value, pv.total()),
        (since(800), v[800], since(801))
    );
    assert_eq!(verus_client::told_by_a_prefix_iters_place(&it), 801);
    assert_eq!(verus_client::told_by_a_prefix_iters_end(&it), 2000);
    assert_eq!(verus_client::told_by_a_prefix_iters_count(&it), (1199, 199));
    assert_eq!(
        verus_client::told_by_a_prefix_size_hint(&it),
        (199, Some(199))
    );
    verus_client::told_of_what_is_left(&it);
    // past what it has
    assert!(verus_client::told_by_a_prefixed_nth(&mut it, 199).is_none());
    assert_eq!(
        (it.pos(), it.end_pos(), it.total()),
        (1000, 1000, since(1000))
    );
    assert!(verus_client::told_by_a_prefixed_next(&mut it).is_none());
    assert!(verus_client::told_by_a_prefix_shift_next(&mut it, 1000..1000).is_none());
    assert_eq!(verus_client::a_step_with_its_sums(&mut it), None);
    verus_client::told_by_a_prefix_advance_to(&mut it, usize::MAX);
    verus_client::told_by_a_prefix_advance_by(&mut it, usize::MAX);
    verus_client::told_by_a_prefix_shift(&mut it, 5..3);
    assert_eq!(
        (it.pos(), it.end_pos(), it.total()),
        (1000, 1000, since(1000))
    );
    verus_client::told_of_what_is_left(&it);
    // an iterator of a column of nothing
    let none = VerifiedPrefixColumn::<Option<i64>>::new();
    let mut it = verus_client::told_by_a_prefix_iter(&none);
    assert!(verus_client::told_by_a_prefixed_next(&mut it).is_none());
    assert_eq!(verus_client::told_of_the_running_sum(&it), 0);
    assert!(verus_client::told_by_a_get_prefixed(&none, 0).is_none());
    let it = verus_client::told_by_a_prefix_iter_range(&none, 0, 0);
    assert_eq!(verus_client::told_by_a_prefix_iters_count(&it), (0, 0));
    assert_eq!(verus_client::told_by_a_prefix_size_hint(&it), (0, Some(0)));
}

#[test]
fn sums_read_by_runs() {
    let v: Vec<u64> = (0..1000).map(|i| i / 7).collect();
    let col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let before = |i: usize| v[..i].iter().map(|x| u128::from(*x)).sum::<u128>();
    assert_eq!(verus_client::summed_by_runs(&col), (before(1000), 1000));
    let none = VerifiedPrefixColumn::<Option<i64>>::new();
    assert_eq!(verus_client::summed_by_runs(&none), (0, 0));
    // run by run, in a window that starts and ends within runs
    let mut it = col.prefix_iter_range(5, 20);
    assert_eq!(
        verus_client::a_run_with_its_sums(&mut it),
        Some((2, 0, before(7)))
    );
    let run = verus_client::told_by_a_prefix_next_run(&mut it).unwrap();
    assert_eq!(
        (
            run.count,
            run.value.value,
            run.value.prefix(),
            run.value.total()
        ),
        (7, 1, before(13), before(14))
    );
    assert_eq!(
        verus_client::a_run_with_its_sums(&mut it),
        Some((6, 2, before(20)))
    );
    assert_eq!(verus_client::a_run_with_its_sums(&mut it), None);
    assert!(verus_client::told_by_a_prefix_next_run(&mut it).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (20, 20, before(20)));
    // the runs without their sums, and a range copied by them
    let runs = verus_client::told_by_a_prefix_runs(col.prefix_iter_range(5, 20));
    let got: Vec<(usize, u64)> = runs.map(|run| (run.count, run.value)).collect();
    assert_eq!(got, [(2, 0), (7, 1), (6, 2)]);
    for (a, b) in [(0, 0), (0, 1000), (3, 700), (1000, 1000)] {
        let copy = verus_client::sums_copied_by_runs(&col, a, b);
        assert_eq!(copy.to_vec(), v[a..b]);
    }
    // signed values and flags
    let signed = VerifiedPrefixColumn::<i64>::from_values(&[-4, -4, -4, 9]);
    assert_eq!(verus_client::summed_by_runs(&signed), (-3, 4));
    let mut it = signed.prefix_iter();
    assert_eq!(
        verus_client::a_run_with_its_sums(&mut it),
        Some((3, -4, -12))
    );
    let flags = VerifiedPrefixColumn::<bool>::from_values(&[true, true, false]);
    assert_eq!(verus_client::summed_by_runs(&flags), (2, 3));
}

#[test]
fn sums_sought() {
    let v: Vec<u64> = (0..1000).map(|i| i % 13).collect();
    let col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let before = |i: usize| v[..i].iter().map(|x| u128::from(*x)).sum::<u128>();
    verus_client::found_with_sums(&VerifiedPrefixColumn::<u64>::from_values(&v[..10]));
    // the first sum past a limit, by the index and by reading on
    for limit in [0, 1, 2, 3, 1000, before(1000) - 1, before(1000), u128::MAX] {
        let first = (0..1000).find(|i| before(i + 1) > limit);
        assert_eq!(
            verus_client::found_by_its_sum(&col, limit),
            first.map(|i| (i, v[i]))
        );
        assert_eq!(
            verus_client::first_over_two_ways(&col, limit),
            (first, first)
        );
    }
    let flags = VerifiedPrefixColumn::<bool>::from_values(&[false, true, false, true]);
    assert_eq!(verus_client::found_by_its_sum(&flags, 0), Some((1, true)));
    assert_eq!(verus_client::found_by_its_sum(&flags, 1), Some((3, true)));
    assert_eq!(verus_client::found_by_its_sum(&flags, 2), None);
    // from where an iterator stands: 9, 10 and 11 come next
    let mut it = col.prefix_iter_range(100, 900);
    let seek = verus_client::told_by_an_advance_prefix(&mut it, 20).unwrap();
    assert_eq!(
        (seek.pos, seek.delta, seek.pv.value, seek.pv.total()),
        (102, 19, 11, before(103))
    );
    assert_eq!(
        verus_client::told_by_a_prefix_seek(&seek),
        (102, 19, seek.pv)
    );
    // by count, with what is passed over
    let seek = verus_client::told_by_a_delta_nth(&mut it, 10).unwrap();
    assert_eq!(
        (seek.pos, seek.delta, seek.pv.value, seek.pv.total()),
        (113, before(113) - before(103), v[113], before(114))
    );
    // past what it has
    assert!(verus_client::told_by_a_delta_nth(&mut it, 786).is_none());
    assert_eq!(
        (it.pos(), it.end_pos(), it.total()),
        (900, 900, before(900))
    );
    assert!(verus_client::told_by_an_advance_prefix(&mut it, 0).is_none());
    // the column asked for a value and the sum of a range before it
    for (a, b) in [(0, 0), (0, 999), (17, 400), (999, 999)] {
        let range = before(b) - before(a);
        assert_eq!(verus_client::a_range_two_ways(&col, a, b), (range, range));
        let seek = verus_client::told_by_a_column_delta(&col, a, b).unwrap();
        assert_eq!((seek.pos, seek.delta, seek.pv.value), (b, range, v[b]));
    }
    assert!(verus_client::told_by_a_column_delta(&col, 3, 1000).is_none());
}

#[test]
fn sums_stopped_and_started() {
    let v: Vec<u64> = (0..1000).map(|i| i % 13).collect();
    let mut col = VerifiedPrefixColumn::<u64>::from_values(&v);
    let before = |i: usize| v[..i].iter().map(|x| u128::from(*x)).sum::<u128>();
    for (a, m, b) in [
        (0, 0, 0),
        (0, 500, 1000),
        (3, 3, 700),
        (10, 990, 990),
        (1000, 1000, 1000),
    ] {
        assert_eq!(
            verus_client::summed_in_two_goes(&col, a, m, b),
            before(b) - before(a)
        );
    }
    // an iterator whose sum counts from within the column
    let mut it = col.prefix_iter_range(100, 900);
    it.nth(49);
    it.reset_prefix();
    it.nth(9);
    let since = before(160) - before(150);
    let s = verus_client::told_by_a_prefix_suspend(&it, &col);
    verus_client::told_by_a_prefix_states_fit(&s, &col);
    let twin = verus_client::told_by_a_prefix_clone(&it);
    let on = verus_client::handed_over_with_its_sum(it, &col);
    assert_eq!((on.pos(), on.end_pos(), on.total()), (160, 900, since));
    assert_eq!(
        (twin.pos(), twin.end_pos(), twin.total()),
        (160, 900, since)
    );
    let mut on = verus_client::told_by_a_prefix_resume(&s, &col).unwrap();
    assert_eq!(
        on.next().map(|pv| (pv.prefix(), pv.total())),
        Some((since, before(161) - before(150)))
    );
    // an iterator of no column
    let no = verus_client::told_by_a_prefix_default::<u64, Leb128>();
    assert_eq!((no.pos(), no.end_pos(), no.len(), no.total()), (0, 0, 0, 0));
    // an edit ends what was suspended
    assert!(!verus_client::sums_ended_by_a_push(&mut col, 7));
    assert!(verus_client::told_by_a_prefix_resume(&s, &col).is_err());
    assert_eq!(col.len(), 1001);
}

/// A thousand of the client's own values: of the kinds 0 to 15 and the
/// lengths 0 to 12, each by turns.
fn some_metas() -> (Vec<Meta>, Vec<u64>) {
    let lens: Vec<u64> = (0..1000).map(|i| i % 13).collect();
    let metas = lens
        .iter()
        .enumerate()
        .map(|(i, len)| verus_client::a_meta((i % 16) as u8, *len))
        .collect();
    (metas, lens)
}

#[test]
fn the_clients_own_sums() {
    let (v, lens) = some_metas();
    let before = |i: usize| lens[..i].iter().sum::<u64>();
    for (i, m) in v.iter().enumerate() {
        assert_eq!(m.0, lens[i] << 4 | (i as u64 % 16));
    }
    assert_eq!(verus_client::lengths_added_up(v[5], v[6], v[7]), (11, 18));
    assert_eq!(verus_client::a_null_has_no_length(v[5], v[12]), 17);

    let col = VerifiedPrefixColumn::<Meta>::from_values(&v);
    let nulls: Vec<Option<Meta>> = v
        .iter()
        .enumerate()
        .map(|(i, m)| if i % 3 == 0 { None } else { Some(*m) })
        .collect();
    let nullable = VerifiedPrefixColumn::<Option<Meta>>::from_values(&nulls);
    let kept = (0..1000)
        .filter(|i| i % 3 != 0)
        .map(|i| lens[i])
        .sum::<u64>();
    for target in [0, 1, 2, 77, before(1000), before(1000) + 1, u64::MAX] {
        let (sums, place, by_runs) = verus_client::sums_of_the_clients_own(&col, &nullable, target);
        assert_eq!(sums.len(), 1000);
        assert!(sums.iter().enumerate().all(|(i, s)| *s == before(i + 1)));
        assert_eq!(place, (0..=1000).find(|i| before(*i) >= target));
        assert_eq!(by_runs, (kept, 1000));
    }
    // the value in which a place of the whole lies
    for n in [0, 1, 4, 5, 77, before(1000) - 1, before(1000), u64::MAX - 1] {
        assert_eq!(
            verus_client::the_value_at(&col, n),
            (0..1000).find(|i| before(i + 1) > n)
        );
    }

    // what loads: any sixteen values, and more whose lengths fit
    let loaded = verus_client::load_lengths(&col.save()).unwrap();
    assert_eq!(loaded.len(), 1000);
    assert_eq!(loaded.get_prefix(1000), before(1000));
    let longest = verus_client::a_meta(9, (1 << 60) - 1);
    let mut plain = VerifiedColumn::<Meta>::fill(16, longest);
    let sixteen = verus_client::load_lengths(&plain.save()).unwrap();
    assert_eq!(sixteen.get_prefix(16), u64::MAX - 15);
    plain.push(verus_client::a_meta(0, 15));
    let full = verus_client::load_lengths(&plain.save()).unwrap();
    assert_eq!((full.len(), full.get_prefix(17)), (17, u64::MAX));
    plain.push(verus_client::a_meta(0, 1));
    assert!(verus_client::load_lengths(&plain.save()).is_none());
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_length_past_the_sums() {
    let longest = verus_client::a_meta(9, (1 << 60) - 1);
    let mut col = VerifiedPrefixColumn::<Meta>::fill(16, longest);
    col.push(verus_client::a_meta(0, 15));
    verus_client::a_length_past_the_sums(&mut col, verus_client::a_meta(3, 1));
}

/// What marks add up to: each opens its kind, or closes it where it is
/// open.
fn flags_of(marks: &[Mark]) -> u64 {
    marks.iter().fold(0, |open, m| open ^ 1 << (m.0 % 64))
}

/// What steps leave of a board: each changes the lamps of its number as
/// the board then lies, and an odd one turns the board round.
fn board_of(steps: &[Step]) -> (u64, bool) {
    steps.iter().fold((0, false), |(lit, turned), s| {
        let changed = s.0 >> 1;
        let changed = if turned {
            changed.rotate_left(32)
        } else {
            changed
        };
        (lit ^ changed, turned != (s.0 & 1 == 1))
    })
}

fn lamps(b: Board) -> (u64, bool) {
    (b.lit, b.turned)
}

/// A step whose lamps lie on both halves of the board, and that turns
/// the board round for one `k` in three.
fn a_step(k: u64) -> Step {
    Step((k % 11) << 31 | (k % 3))
}

#[test]
fn accumulators() {
    // seven marks of a kind at a time, and five equal steps
    let marks: Vec<Mark> = (0..1000).map(|i| Mark(i / 7 % 5 * 13)).collect();
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let col = VerifiedAccColumn::<Mark>::from_values(&marks);
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let plain = VerifiedColumn::<Mark>::from_values(&marks);
    let bytes = plain.save();

    // the clients of a column of any kind, of accumulator columns
    verus_client::round_trip::<Mark, Leb128, AccIndex>(&marks);
    verus_client::round_trip::<Option<Step>, Leb128, AccIndex>(&vec![
        Some(Step(3)),
        None,
        None,
        Some(Step(4)),
    ]);
    verus_client::load_then_save::<Mark, Leb128, AccIndex>(&bytes);
    verus_client::clone_then_diverge::<Step, Leb128, AccIndex>(&steps, Step(77));
    verus_client::collect_then_extend::<Mark, Leb128, AccIndex>(
        vec![Mark(1), Mark(1), Mark(2)],
        vec![Mark(3)],
        vec![Mark(4), Mark(4)],
    );
    verus_client::slabs_and_validation::<Mark, Leb128, AccIndex>(&(0..10_000).map(Mark).collect());

    // by hand: the kinds 0, 13 and 26, and one kind opened and closed
    let (two, three) = verus_client::marks_added_up(marks[5], marks[7], marks[14]);
    assert_eq!((two.0, three.0), (0x2001, 0x400_2001));
    assert_eq!(verus_client::a_null_adds_no_mark(Mark(3), Mark(5)).0, 0x28);
    assert_eq!(verus_client::a_null_adds_no_mark(Mark(3), Mark(67)).0, 0);
    let (one_way, the_other) = verus_client::steps_in_order();
    assert_eq!(lamps(one_way), (0x2_0000_0001, true));
    assert_eq!(lamps(the_other), (3, true));
    assert_eq!(lamps(one_way), board_of(&[Step(3), Step(4)]));
    assert_eq!(lamps(the_other), board_of(&[Step(4), Step(3)]));

    // what each of the three answers says, also past the end
    for i in [0, 1, 7, 999, 1000, 1001, usize::MAX] {
        let (to, through) = (i.min(1000), i.saturating_add(1).min(1000));
        let mark = verus_client::told_by_an_acc_prefix(&col, i);
        let step = verus_client::told_by_an_acc_prefix(&board, i);
        assert_eq!(
            (mark.0, lamps(step)),
            (flags_of(&marks[..to]), board_of(&steps[..to]))
        );
        let mark = verus_client::told_by_an_acc_total(&col, i);
        let step = verus_client::told_by_an_acc_total(&board, i);
        assert_eq!(mark.0, flags_of(&marks[..through]));
        assert_eq!(lamps(step), board_of(&steps[..through]));
    }
    let places = [(0, 0), (0, 1000), (3, 3), (6, 8), (123, 987), (1000, 1000)];
    let past = [(5, 3), (3, 2000), (1000, 1001), (2000, 3000), (2000, 0)];
    for (start, end) in places.into_iter().chain(past) {
        let range = if start >= end || start >= 1000 {
            0..0
        } else {
            start..end.min(1000)
        };
        let mark = verus_client::told_by_an_acc_range(&col, start, end);
        let step = verus_client::told_by_an_acc_range(&board, start, end);
        assert_eq!(mark.0, flags_of(&marks[range.clone()]));
        assert_eq!(lamps(step), board_of(&steps[range]));
    }

    // a range after what is before it, and by taking that away
    for (start, end) in places {
        let (before, range, through) = verus_client::a_range_added_after(&col, start, end);
        assert_eq!(before.0, flags_of(&marks[..start]));
        assert_eq!(range.0, flags_of(&marks[start..end]));
        assert_eq!(through.0, flags_of(&marks[..end]));
        let taken = verus_client::a_range_by_taking_away(&col, start, end);
        assert_eq!(taken.0, range.0);

        let (before, range, through) = verus_client::a_range_added_after(&board, start, end);
        assert_eq!(lamps(before), board_of(&steps[..start]));
        assert_eq!(lamps(range), board_of(&steps[start..end]));
        assert_eq!(lamps(through), board_of(&steps[..end]));
        let taken = verus_client::a_range_by_taking_away(&board, start, end);
        assert_eq!(lamps(taken), lamps(range));
    }

    // whatever loads as a plain column loads as one, and nothing else
    let loaded = verus_client::load_as_an_acc::<Mark, Leb128>(&bytes).unwrap();
    assert_eq!((loaded.len(), loaded.save()), (1000, bytes.clone()));
    assert_eq!(loaded.get_prefix(1000).0, flags_of(&marks));
    let cut = &bytes[..bytes.len() - 1];
    for b in [cut, &[], &[1], &[0x7f], &[2, 5, 2, 5]] {
        let as_one = verus_client::load_as_an_acc::<Mark, Leb128>(b);
        assert_eq!(as_one.is_some(), VerifiedColumn::<Mark>::load(b).is_ok());
    }
    assert!(verus_client::load_as_an_acc::<Mark, Leb128>(&[1]).is_none());
    let up = verus_client::added_up(&plain);
    assert_eq!((up.len(), up.save()), (1000, bytes));
    assert_eq!(up.get_total(999).0, flags_of(&marks));

    // a mark put in adds its kind to what is before it
    let mut more = col.clone();
    let with_it = verus_client::a_mark_put_in(&mut more, 500, Mark(63));
    assert_eq!(with_it.0, flags_of(&marks[..500]) ^ 1 << 63);
    assert_eq!((more.len(), more.get_prefix(501).0), (1001, with_it.0));
    assert_eq!(more.get_prefix(1001).0, flags_of(&marks) ^ 1 << 63);
    assert_eq!(col.get_prefix(1000).0, flags_of(&marks));
}

#[test]
fn accumulators_by_runs() {
    let marks: Vec<Mark> = (0..1000).map(|i| Mark(i / 7 % 5 * 13)).collect();
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let col = VerifiedAccColumn::<Mark>::from_values(&marks);
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let by_runs = verus_client::added_by_runs::<Mark, Leb128>(&col);
    assert_eq!(by_runs.0, flags_of(&marks));
    let by_runs = verus_client::added_by_runs::<Step, Leb128>(&board);
    assert_eq!(lamps(by_runs), board_of(&steps));

    // long runs: a mark an odd number of times over is open, and a step
    // is taken one time after another
    for n in [0, 1, 2, 4_000, 100_001] {
        let long = VerifiedAccColumn::<Mark>::fill(n, Mark(9));
        let by_runs = verus_client::added_by_runs::<Mark, Leb128>(&long);
        assert_eq!(by_runs.0, if n % 2 == 1 { 1 << 9 } else { 0 });
        let same = vec![a_step(4); n];
        let long = VerifiedAccColumn::<Step>::from_values(&same);
        let by_runs = verus_client::added_by_runs::<Step, Leb128>(&long);
        assert_eq!(lamps(by_runs), board_of(&same));
    }

    // adding a number of times over: in a way of the type's own, and as
    // the trait gives it
    for n in [0, 1, 2, 5, 64] {
        let mut open = Flags(0xf0);
        verus_client::told_by_add_times(&mut open, &Flags(0x3c), n);
        assert_eq!(open.0, if n % 2 == 1 { 0xcc } else { 0xf0 });

        let mut b = Board {
            lit: 7,
            turned: true,
        };
        let step = Board {
            lit: 1 << 32 | 2,
            turned: true,
        };
        verus_client::told_by_add_times(&mut b, &step, n);
        let mut steps = vec![Step(15)];
        steps.resize(n + 1, Step(1 << 33 | 5));
        assert_eq!(lamps(b), board_of(&steps));
    }

    // an accumulator's own functions, and what was added found again by
    // taking away what there was before it
    let none = verus_client::told_by_new_zero::<Board>();
    assert_eq!(lamps(none), (0, false));
    let (first, then) = (Step(15), Step(1 << 33 | 5));
    let step = verus_client::told_by_acc_of(then);
    let mut b = verus_client::told_by_dup(&none);
    verus_client::told_by_add_acc(&mut b, &verus_client::told_by_acc_of(first));
    let before = verus_client::told_by_dup(&b);
    verus_client::told_by_add_acc(&mut b, &step);
    assert_eq!(lamps(b), board_of(&[first, then]));
    assert_eq!(lamps(before), board_of(&[first]));
    let added = verus_client::added_then_taken_away(&before, &step);
    assert_eq!(lamps(added), lamps(step));
    verus_client::told_by_sub_acc(&mut b, &before);
    assert_eq!(lamps(b), lamps(step));
    let open = verus_client::told_by_acc_of(Mark(70));
    let both = verus_client::added_then_taken_away(&Flags(0xf0), &open);
    assert_eq!((open.0, both.0), (0x40, 0x40));
}

/// A mark an accumulator column's iterator gives, with the kinds open
/// before it and through it.
fn mark_with_sums(pv: &VerifiedAccValue<'_, Mark>) -> (u64, u64, u64) {
    (pv.prefix().0, pv.value.0, pv.total().0)
}

/// A step an accumulator column's iterator gives, with the board before
/// it and through it.
fn step_with_sums(pv: &VerifiedAccValue<'_, Step>) -> ((u64, bool), u64, (u64, bool)) {
    (lamps(pv.prefix()), pv.value.0, lamps(pv.total()))
}

/// Where an iterator over steps stands, where its window ends, and the
/// board of the steps it has passed.
fn stands(it: &VerifiedAccIter<'_, Step>) -> (usize, usize, (u64, bool)) {
    (it.pos(), it.end_pos(), lamps(it.total()))
}

#[test]
fn accumulators_read_in_step() {
    let marks: Vec<Mark> = (0..1000).map(|i| Mark(i / 7 % 5 * 13)).collect();
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let col = VerifiedAccColumn::<Mark>::from_values(&marks);
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let open = |i: usize| flags_of(&marks[..i]);
    let lit = |i: usize| board_of(&steps[..i]);
    // each running sum, of the column and of a range
    let sums = verus_client::running_accs(&col);
    let sums: Vec<u64> = sums.iter().map(|f| f.0).collect();
    assert_eq!(sums, (1..=1000).map(open).collect::<Vec<_>>());
    let sums = verus_client::running_accs(&board);
    let sums: Vec<(u64, bool)> = sums.into_iter().map(lamps).collect();
    assert_eq!(sums, (1..=1000).map(lit).collect::<Vec<_>>());
    assert!(verus_client::running_accs(&VerifiedAccColumn::<Option<Mark>>::new()).is_empty());
    for (a, b) in [(0, 0), (0, 1000), (3, 700), (999, 1000), (1000, 1000)] {
        let got: Vec<(u64, (u64, bool))> = verus_client::accs_in_a_range(&board, a, b)
            .into_iter()
            .map(|(step, before)| (step.0, lamps(before)))
            .collect();
        let want: Vec<(u64, (u64, bool))> = (a..b).map(|i| (steps[i].0, lit(i))).collect();
        assert_eq!(got, want);
        let range = board_of(&steps[a..b]);
        let (moved, asked) = verus_client::added_by_moving(&board, a, b);
        assert_eq!((lamps(moved), lamps(asked)), (range, range));
        let range = flags_of(&marks[a..b]);
        let (moved, asked) = verus_client::added_by_moving(&col, a, b);
        assert_eq!((moved.0, asked.0), (range, range));
    }
    // one value with its sums
    for i in [0, 1, 500, 999] {
        let sums = (lit(i), steps[i].0, lit(i + 1));
        let (before, v, through) = verus_client::a_value_with_its_accs(&board, i);
        assert_eq!((lamps(before), v.0, lamps(through)), sums);
        let (given, asked) = verus_client::accs_two_ways(&board, i);
        assert_eq!((lamps(given.0), lamps(given.1)), (sums.0, sums.2));
        assert_eq!((lamps(asked.0), lamps(asked.1)), (sums.0, sums.2));
        let sums = (open(i), marks[i].0, open(i + 1));
        let pv = verus_client::told_by_an_acc_get_prefixed(&col, i).unwrap();
        assert_eq!(mark_with_sums(&pv), sums);
        assert_eq!(verus_client::told_by_an_acc_values_total(&pv).0, sums.2);
        assert_eq!(verus_client::told_by_an_acc_values_prefix(&pv).0, sums.0);
        let twin = verus_client::told_by_an_acc_values_clone(&pv);
        assert_eq!(mark_with_sums(&twin), sums);
    }
    assert!(verus_client::told_by_an_acc_get_prefixed(&col, 1000).is_none());
    for (i, j) in [(0, 1), (0, 999), (17, 400), (998, 999)] {
        let (first, then) = verus_client::two_skips_added(&board, i, j);
        assert_eq!((lamps(first), lamps(then)), (lit(i + 1), lit(j + 1)));
    }
    // step by step, to the end
    let mut it = verus_client::told_by_an_acc_iter_range(&board, 998, 1000);
    for i in [998, 999] {
        let (before, v, through) = verus_client::a_step_with_its_accs(&mut it).unwrap();
        let sums = (lit(i), steps[i].0, lit(i + 1));
        assert_eq!((lamps(before), v.0, lamps(through)), sums);
    }
    assert!(verus_client::a_step_with_its_accs(&mut it).is_none());
    // two steps in either order: the board before the second, and after
    let (one_way, the_other) = verus_client::steps_read_in_order();
    assert_eq!(lamps(one_way.0), board_of(&[Step(3)]));
    assert_eq!(lamps(one_way.1), board_of(&[Step(3), Step(4)]));
    assert_eq!(lamps(the_other.0), board_of(&[Step(4)]));
    assert_eq!(lamps(the_other.1), board_of(&[Step(4), Step(3)]));
    assert_eq!(
        (lamps(one_way.1), lamps(the_other.1)),
        ((0x2_0000_0001, true), (3, true))
    );
    // nulls, which add nothing
    let nullable = VerifiedAccColumn::<Option<Mark>>::from_values(&[
        None,
        Some(Mark(3)),
        None,
        Some(Mark(5)),
        Some(Mark(3)),
    ]);
    let sums = verus_client::running_accs(&nullable);
    let sums: Vec<u64> = sums.iter().map(|f| f.0).collect();
    assert_eq!(sums, [0, 8, 8, 0x28, 0x20]);
    let (before, v, through) = verus_client::a_value_with_its_accs(&nullable, 2);
    assert_eq!((before.0, v.map(|m| m.0), through.0), (8, None, 8));
}

#[test]
fn accumulators_kept_by_each_move() {
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let lit = |i: usize| board_of(&steps[..i]);
    verus_client::moved_with_accs(&VerifiedAccColumn::<Step>::from_values(&steps[..10]));

    let mut it = verus_client::told_by_an_acc_iter(&board);
    let sum = verus_client::told_of_the_running_acc(&it);
    let before = verus_client::told_of_the_acc_before(&it);
    assert_eq!((lamps(sum), lamps(before)), ((0, false), (0, false)));
    assert_eq!(verus_client::told_by_an_acc_iters_place(&it), 0);
    assert_eq!(verus_client::told_by_an_acc_iters_end(&it), 1000);
    assert_eq!(verus_client::told_by_an_acc_iters_reach(&it), 1000);
    assert_eq!(verus_client::told_by_an_acc_iters_len(&it), 1000);
    assert_eq!(
        verus_client::told_by_an_acc_size_hint(&it),
        (1000, Some(1000))
    );
    verus_client::told_of_the_accs_left(&it);
    // read, and stepped over
    let pv = verus_client::told_by_an_acc_next(&mut it).unwrap();
    assert_eq!(step_with_sums(&pv), ((0, false), steps[0].0, lit(1)));
    let pv = verus_client::told_by_an_acc_nth(&mut it, 499).unwrap();
    assert_eq!(step_with_sums(&pv), (lit(500), steps[500].0, lit(501)));
    let sum = verus_client::told_of_the_running_acc(&it);
    let before = verus_client::told_of_the_acc_before(&it);
    assert_eq!((lamps(sum), lamps(before)), (lit(501), lit(501)));
    // its sum counts from the column's start, where the column's own do
    let (kept, asked) = verus_client::a_running_acc_two_ways(&it, &board);
    assert_eq!((lamps(kept), lamps(asked)), (lit(501), lit(501)));
    // the window's end moved, and the iterator
    verus_client::told_by_an_acc_set_max(&mut it, 900);
    assert_eq!(verus_client::told_by_an_acc_iters_reach(&it), 399);
    assert_eq!(verus_client::told_by_an_acc_iters_len(&it), 399);
    assert_eq!(
        verus_client::told_by_an_acc_size_hint(&it),
        (399, Some(399))
    );
    verus_client::told_by_an_acc_advance_to(&mut it, 600);
    assert_eq!(stands(&it), (600, 900, lit(600)));
    verus_client::told_by_an_acc_advance_by(&mut it, 50);
    assert_eq!(stands(&it), (650, 900, lit(650)));
    // its sum started again
    verus_client::told_by_an_acc_reset(&mut it);
    assert_eq!(stands(&it), (650, 900, (0, false)));
    let since = |i: usize| board_of(&steps[650..i]);
    verus_client::told_by_an_acc_shift(&mut it, 700..950);
    assert_eq!(stands(&it), (700, 950, since(700)));
    // a window that ends past the column
    let pv = verus_client::told_by_an_acc_shift_next(&mut it, 800..2000).unwrap();
    assert_eq!(step_with_sums(&pv), (since(800), steps[800].0, since(801)));
    assert_eq!(verus_client::told_by_an_acc_iters_place(&it), 801);
    assert_eq!(verus_client::told_by_an_acc_iters_end(&it), 2000);
    assert_eq!(verus_client::told_by_an_acc_iters_reach(&it), 1199);
    assert_eq!(verus_client::told_by_an_acc_iters_len(&it), 199);
    assert_eq!(
        verus_client::told_by_an_acc_size_hint(&it),
        (199, Some(199))
    );
    verus_client::told_of_the_accs_left(&it);
    // past what it has
    assert!(verus_client::told_by_an_acc_nth(&mut it, 199).is_none());
    assert_eq!(stands(&it), (1000, 1000, since(1000)));
    assert!(verus_client::told_by_an_acc_next(&mut it).is_none());
    assert!(verus_client::told_by_an_acc_shift_next(&mut it, 1000..1000).is_none());
    assert!(verus_client::a_step_with_its_accs(&mut it).is_none());
    verus_client::told_by_an_acc_advance_to(&mut it, usize::MAX);
    verus_client::told_by_an_acc_advance_by(&mut it, usize::MAX);
    verus_client::told_by_an_acc_shift(&mut it, 5..3);
    assert_eq!(stands(&it), (1000, 1000, since(1000)));
    verus_client::told_of_the_accs_left(&it);
    // an iterator of a column of nothing
    let none = VerifiedAccColumn::<Option<Mark>>::new();
    let mut it = verus_client::told_by_an_acc_iter(&none);
    assert!(verus_client::told_by_an_acc_next(&mut it).is_none());
    assert_eq!(verus_client::told_of_the_running_acc(&it).0, 0);
    assert!(verus_client::told_by_an_acc_get_prefixed(&none, 0).is_none());
    let it = verus_client::told_by_an_acc_iter_range(&none, 0, 0);
    assert_eq!(verus_client::told_by_an_acc_iters_len(&it), 0);
    assert_eq!(verus_client::told_by_an_acc_size_hint(&it), (0, Some(0)));
}

#[test]
fn accumulators_read_by_runs() {
    let marks: Vec<Mark> = (0..1000).map(|i| Mark(i / 7 % 5 * 13)).collect();
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let col = VerifiedAccColumn::<Mark>::from_values(&marks);
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let open = |i: usize| flags_of(&marks[..i]);
    let lit = |i: usize| board_of(&steps[..i]);
    let (sum, count) = verus_client::added_by_run_moves(&col);
    assert_eq!((sum.0, count), (open(1000), 1000));
    let (sum, count) = verus_client::added_by_run_moves(&board);
    assert_eq!((lamps(sum), count), (lit(1000), 1000));
    let none = VerifiedAccColumn::<Option<Mark>>::new();
    let (sum, count) = verus_client::added_by_run_moves(&none);
    assert_eq!((sum.0, count), (0, 0));
    // run by run, in a window that starts and ends within runs
    let mut it = col.prefix_iter_range(5, 20);
    let (n, v, through) = verus_client::a_run_with_its_accs(&mut it).unwrap();
    assert_eq!((n, v.0, through.0), (2, 0, open(7)));
    let run = verus_client::told_by_an_acc_next_run(&mut it).unwrap();
    assert_eq!(run.count, 7);
    assert_eq!(mark_with_sums(&run.value), (open(13), 13, open(14)));
    let (n, v, through) = verus_client::a_run_of_marks(&mut it).unwrap();
    assert_eq!((n, v.0, through.0), (6, 26, open(20)));
    assert!(verus_client::a_run_with_its_accs(&mut it).is_none());
    assert!(verus_client::told_by_an_acc_next_run(&mut it).is_none());
    assert!(verus_client::a_run_of_marks(&mut it).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total().0), (20, 20, open(20)));
    // a run of an odd number of marks opens their kind
    let mut it = col.prefix_iter_range(7, 14);
    let (n, v, through) = verus_client::a_run_of_marks(&mut it).unwrap();
    assert_eq!((n, v.0, through.0), (7, 13, open(7) ^ 1 << 13));
    // steps: each of a run is taken after the one before it
    let mut it = board.prefix_iter_range(3, 12);
    for (count, to) in [(2, 5), (5, 10), (2, 12)] {
        let (n, v, through) = verus_client::a_run_with_its_accs(&mut it).unwrap();
        assert_eq!((n, v.0, lamps(through)), (count, steps[to - 1].0, lit(to)));
    }
    assert!(verus_client::a_run_with_its_accs(&mut it).is_none());
    // the runs without their sums, and a range copied by them
    let runs = verus_client::told_by_an_acc_runs(col.prefix_iter_range(5, 20));
    let got: Vec<(usize, u64)> = runs.map(|run| (run.count, run.value.0)).collect();
    assert_eq!(got, [(2, 0), (7, 13), (6, 26)]);
    for (a, b) in [(0, 0), (0, 1000), (3, 700), (1000, 1000)] {
        let copy = verus_client::accs_copied_by_runs(&col, a, b);
        let got: Vec<u64> = copy.to_vec().iter().map(|m| m.0).collect();
        let want: Vec<u64> = marks[a..b].iter().map(|m| m.0).collect();
        assert_eq!(got, want);
    }
}

#[test]
fn accumulators_sought() {
    let marks: Vec<Mark> = (0..10).map(|i| Mark(i / 7 % 5 * 13)).collect();
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let lit = |i: usize| board_of(&steps[..i]);
    verus_client::found_with_accs(&VerifiedAccColumn::<Mark>::from_values(&marks));
    // by count, with what is passed over
    let mut it = board.prefix_iter_range(100, 900);
    let seek = verus_client::told_by_an_acc_delta_nth(&mut it, 10).unwrap();
    let passed = board_of(&steps[100..110]);
    let found = (lit(110), steps[110].0, lit(111));
    assert_eq!((seek.pos, lamps(seek.delta)), (110, passed));
    assert_eq!(step_with_sums(&seek.pv), found);
    let twin = verus_client::told_by_an_acc_seek(&seek);
    assert_eq!((twin.pos, lamps(twin.delta)), (110, passed));
    assert_eq!(step_with_sums(&twin.pv), found);
    let (pos, delta, v, total) = verus_client::the_parts_of_a_find(seek);
    assert_eq!((pos, lamps(delta)), (110, passed));
    assert_eq!((v.0, lamps(total)), (steps[110].0, lit(111)));
    // past what it has
    assert!(verus_client::told_by_an_acc_delta_nth(&mut it, 789).is_none());
    assert_eq!(stands(&it), (900, 900, lit(900)));
    // the column asked for a value and the sum of a range before it
    for (a, b) in [(0, 0), (0, 999), (17, 400), (999, 999)] {
        let range = board_of(&steps[a..b]);
        let (found, asked) = verus_client::a_range_added_two_ways(&board, a, b);
        assert_eq!((lamps(found), lamps(asked)), (range, range));
        let seek = verus_client::told_by_an_acc_delta(&board, a, b).unwrap();
        assert_eq!((seek.pos, lamps(seek.delta)), (b, range));
        assert_eq!(step_with_sums(&seek.pv), (lit(b), steps[b].0, lit(b + 1)));
    }
    assert!(verus_client::told_by_an_acc_delta(&board, 3, 1000).is_none());
}

#[test]
fn accumulators_stopped_and_started() {
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    let mut board = VerifiedAccColumn::<Step>::from_values(&steps);
    for (a, m, b) in [
        (0, 0, 0),
        (0, 500, 1000),
        (3, 3, 700),
        (10, 990, 990),
        (1000, 1000, 1000),
    ] {
        let sum = verus_client::added_in_two_goes(&board, a, m, b);
        assert_eq!(lamps(sum), board_of(&steps[a..b]));
    }
    // an iterator whose sum counts from within the column
    let mut it = board.prefix_iter_range(100, 900);
    it.nth(49);
    it.reset_prefix();
    it.nth(9);
    let since = board_of(&steps[150..160]);
    let s = verus_client::told_by_an_acc_suspend(&it, &board);
    verus_client::told_by_an_acc_states_fit(&s, &board);
    let twin = verus_client::told_by_an_acc_clone(&it);
    let on = verus_client::handed_over_with_its_acc(it, &board);
    assert_eq!(stands(&on), (160, 900, since));
    assert_eq!(stands(&twin), (160, 900, since));
    let mut on = verus_client::told_by_an_acc_resume(&s, &board).unwrap();
    let pv = on.next().unwrap();
    assert_eq!(
        (lamps(pv.prefix()), lamps(pv.total())),
        (since, board_of(&steps[150..161]))
    );
    // an iterator of no column
    let no = verus_client::told_by_an_acc_default::<Step, Leb128>();
    assert_eq!((stands(&no), no.len()), ((0, 0, (0, false)), 0));
    // an edit ends what was suspended
    assert!(!verus_client::accs_ended_by_a_push(&mut board, Step(7)));
    assert!(verus_client::told_by_an_acc_resume(&s, &board).is_err());
    assert_eq!(board.len(), 1001);
}

#[test]
fn the_clients_own_accumulators() {
    let marks: Vec<Mark> = (0..1000).map(|i| Mark(i / 7 % 5 * 13)).collect();
    let steps: Vec<Step> = (0..1000).map(|i| a_step(i / 5)).collect();
    // marks of which every third is none
    let some: Vec<Option<Mark>> = (0..1000)
        .map(|i| (i % 3 != 0).then_some(marks[i]))
        .collect();
    let there: Vec<Mark> = some.iter().flatten().copied().collect();
    let col = VerifiedAccColumn::<Mark>::from_values(&marks);
    let nullable = VerifiedAccColumn::<Option<Mark>>::from_values(&some);
    let board = VerifiedAccColumn::<Step>::from_values(&steps);
    let (sums, (open, count), (moved, asked)) =
        verus_client::accs_of_the_clients_own(&col, &nullable, &board, 123, 987);
    let sums: Vec<u64> = sums.iter().map(|f| f.0).collect();
    let want: Vec<u64> = (1..=1000).map(|i| flags_of(&marks[..i])).collect();
    assert_eq!(sums, want);
    assert_eq!((open.0, count), (flags_of(&there), 1000));
    let range = board_of(&steps[123..987]);
    assert_eq!((lamps(moved), lamps(asked)), (range, range));
}

#[test]
fn delta_columns() {
    let values: Vec<u64> = vec![3, 4, 5, 5, 1 << 40, 7];
    let mut col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    assert_eq!(verus_client::delta_values(&col), values);
    assert_eq!(verus_client::delta_len(&col), 6);
    verus_client::delta_set(&mut col, 1, 9);
    verus_client::delta_push_u64(&mut col, 2);
    verus_client::delta_spliced(&mut col, 2, 3, &[8, 8]);
    assert_eq!(verus_client::delta_values(&col), vec![3, 9, 8, 8, 7, 2]);
    assert_eq!(verus_client::delta_read(&col, 4), Some(7));
    assert_eq!(verus_client::delta_read_fits(&col, 6), None);

    let mut opts = verus_client::delta_new::<Option<i32>, Leb128>();
    assert!(verus_client::delta_is_empty(&opts));
    let nums = [Some(-5), None, Some(i32::MAX), Some(i32::MIN), None];
    for v in nums {
        verus_client::delta_push_opt(&mut opts, v);
    }
    assert_eq!(verus_client::delta_values(&opts), nums.to_vec());
}

#[test]
fn delta_simple_edits() {
    let mut col: VerifiedDeltaColumn<i64> = verus_client::delta_small_slabs(&[5, 6, 7, -9, -9, 40]);
    assert_eq!(verus_client::delta_push_pop(&mut col, 41), Some(41));
    verus_client::delta_insert_remove(&mut col, 3, 1 << 50);
    assert_eq!(verus_client::delta_inserted(&mut col, 0, 4, 1), Some(5));
    verus_client::delta_remove_past(&mut col);
    assert_eq!(
        verus_client::delta_round_trip::<i64, Leb128>(&[4, 5, 6, 7, -9, -9, 40]),
        col.to_vec()
    );
    assert_eq!(verus_client::delta_ends(&col), (Some(4), Some(40)));
    assert!(verus_client::delta_all(&col, 6));
    verus_client::delta_remove_range(&mut col, 1, 3);
    assert_eq!(verus_client::delta_truncated(&mut col, 3, 2), Some(-9));
    let other = verus_client::delta_cloned(&col, 8);
    assert_eq!(verus_client::delta_values(&other), vec![4, -9, -9, 8]);
    assert_eq!(verus_client::delta_values(&col), vec![4, -9, -9]);

    let mut to: VerifiedDeltaColumn<i64> = verus_client::delta_default();
    verus_client::delta_move_last(&mut col, &mut to);
    verus_client::delta_move_last(&mut col, &mut to);
    assert_eq!(verus_client::delta_values(&to), vec![-9, -9]);
    verus_client::delta_clear_counts(&mut to);
    verus_client::delta_move_last(&mut to, &mut col);
    assert_eq!(verus_client::delta_values(&col), vec![4]);
    verus_client::delta_cleared(&mut col, &[1, 2]);
    assert_eq!(verus_client::delta_values(&col), vec![1, 2]);
    verus_client::delta_splice_counts(&mut col, 1, 1, &[9, 9]);
    verus_client::delta_splice_counts(&mut col, 0, 0, &[]);
    assert_eq!(verus_client::delta_values(&col), vec![1, 9, 9]);
    assert_eq!(verus_client::delta_pop_empty::<Option<u32>, Leb128>(), None);
}

#[test]
fn delta_saved_and_loaded() {
    let col: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[5, 6, 7, -9, -9, 40]);
    let bytes = verus_client::delta_saved(&col);
    let back = verus_client::delta_save_then_load(&col).unwrap();
    assert_eq!(verus_client::delta_values(&back), vec![5, 6, 7, -9, -9, 40]);
    verus_client::delta_load_then_save::<i64, Leb128>(&bytes);
    // not a column of these: there is nothing to save again
    verus_client::delta_load_then_save::<u32, Leb128>(&bytes);
    let (a, b) = verus_client::delta_same_values_same_bytes(&col, &back);
    assert_eq!(a, b);
    assert_eq!(a, bytes);
    assert!(verus_client::delta_empty_saves_nothing::<Option<u64>, Leb128>().is_empty());
    assert!(verus_client::delta_load_no_bytes::<u32, Leb128>()
        .unwrap()
        .is_empty());

    let mut out = vec![9u8];
    let r = verus_client::delta_saved_after(&col, &mut out);
    assert_eq!(out[r], bytes[..]);
    let other: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[1, 1]);
    let second = verus_client::delta_saved_in_one_buffer(&col, &other).unwrap();
    assert_eq!(verus_client::delta_values(&second), vec![1, 1]);

    assert!(!verus_client::delta_only(&col, 5));
    assert!(verus_client::delta_only(&other, 1));
    verus_client::delta_one_value_only::<Option<i32>, Leb128>(None, Some(-4));
    verus_client::delta_one_value_only::<u64, Leb128>(3, 4);
    let mut out = vec![];
    let (r0, r1) = verus_client::delta_only_nulls(8, &mut out);
    assert!(r0.is_empty());
    assert_eq!(r1, 0..out.len());
    assert!(!out.is_empty());

    // a column with a negative number is none of `u64`s
    assert_eq!(verus_client::delta_loaded_fits(&bytes, 0), None);
    let small: VerifiedDeltaColumn<u64> = verus_client::delta_built(&[4, 2]);
    let small = verus_client::delta_saved(&small);
    assert_eq!(verus_client::delta_loaded_fits(&small, 1), Some(2));
    let edited = verus_client::delta_loaded_edits::<i64, Leb128>(&bytes).unwrap();
    assert_eq!(
        verus_client::delta_values(&edited),
        vec![5, 6, 7, -9, -9, 40]
    );
}

#[test]
fn delta_runs_and_fills() {
    use hexane::DeltaRun;
    let mut col: VerifiedDeltaColumn<u32> = verus_client::delta_built(&[1, 2]);
    verus_client::delta_run_of_three(&mut col);
    assert_eq!(verus_client::delta_values(&col), vec![1, 2, 12, 14, 16]);
    let mut col: VerifiedDeltaColumn<i32> = verus_client::delta_built(&[7]);
    verus_client::delta_run_falls(&mut col);
    assert_eq!(verus_client::delta_values(&col), vec![-5, -10, 7]);

    // a run of `u32`s that falls below 0 is refused, and one that stops
    // short of it is taken
    let mut col: VerifiedDeltaColumn<u32> = verus_client::delta_built(&[1, 2]);
    let low = DeltaRun {
        prefix: 5,
        delta: Some(-3),
        count: 2,
    };
    assert!(!verus_client::delta_run_checked(&mut col, 1, low));
    let run = DeltaRun {
        prefix: 5,
        delta: Some(-2),
        count: 2,
    };
    assert!(verus_client::delta_run_checked(&mut col, 1, run));
    assert_eq!(verus_client::delta_values(&col), vec![1, 3, 1, 2]);

    let mut col: VerifiedDeltaColumn<Option<u64>> =
        verus_client::delta_built(&[Some(1), None, Some(9)]);
    let a = DeltaRun {
        prefix: 100,
        delta: Some(0),
        count: 2,
    };
    let b = DeltaRun {
        prefix: 0,
        delta: None,
        count: 1,
    };
    verus_client::delta_two_runs(&mut col, 1, 1, a, b);
    let values = vec![Some(1), Some(100), Some(100), None, Some(9)];
    assert_eq!(verus_client::delta_values(&col), values);
    verus_client::delta_no_runs(&mut col, 2, -7, Some(3));
    assert_eq!(verus_client::delta_values(&col), values);

    let mut col: VerifiedDeltaColumn<Option<i64>> = verus_client::delta_built(&[Some(4)]);
    verus_client::delta_null_run(&mut col, 99);
    assert_eq!(verus_client::delta_values(&col), vec![None, None, Some(4)]);
    let mut col: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[-5]);
    let over = DeltaRun {
        prefix: i64::MAX,
        delta: Some(1),
        count: 1,
    };
    assert!(verus_client::delta_run_wraps(&mut col, over));
    assert_eq!(verus_client::delta_values(&col), vec![-5, i64::MIN]);
    let nulls = DeltaRun {
        prefix: 0,
        delta: None,
        count: 3,
    };
    assert!(!verus_client::delta_run_wraps(&mut col, nulls));
    assert_eq!(verus_client::delta_values(&col), vec![-5, i64::MIN]);

    let filled = verus_client::delta_load_a_fill::<Leb128>().unwrap();
    assert_eq!(verus_client::delta_values(&filled), vec![2, 4, 6]);
    let nulls = verus_client::delta_load_null_fill().unwrap();
    assert_eq!(verus_client::delta_values(&nulls), vec![None, None, None]);
    let bytes = verus_client::delta_saved(&filled);
    let (a, b) = verus_client::delta_load_with_length::<u64, Leb128>(&bytes, 3, 9);
    assert_eq!(verus_client::delta_values(&a.unwrap()), vec![2, 4, 6]);
    assert_eq!(verus_client::delta_values(&b.unwrap()), vec![2, 4, 6]);
    let (a, b) = verus_client::delta_load_with_length::<u64, Leb128>(&bytes, 4, 9);
    assert!(a.is_none() && b.is_none());
    let (a, b) = verus_client::delta_load_with_length::<u64, Leb128>(&[], 4, 9);
    assert!(a.is_none());
    assert_eq!(verus_client::delta_values(&b.unwrap()), vec![9, 18, 27, 36]);
    let back = verus_client::delta_load_with_defaults::<u64, Leb128>(&bytes, 2).unwrap();
    assert_eq!(verus_client::delta_values(&back), vec![2, 4, 6]);
    let (a, b) = verus_client::delta_fill_is_a_run(5, 3).unwrap();
    assert_eq!(verus_client::delta_values(&a), vec![3, 6, 9, 12, 15]);
    assert_eq!(verus_client::delta_values(&b), vec![3, 6, 9, 12, 15]);
    // a fill that falls is no column of `u64`
    assert!(verus_client::delta_fill_is_a_run(5, -3).is_none());
}

#[test]
fn delta_iterators() {
    use hexane::verified::VerifiedDeltaIter;
    let values: [u64; 10] = [1, 2, 2, 5, 9, 9, 12, 20, 21, 30];
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    assert_eq!(verus_client::delta_read_in_a_loop(&col), values);
    assert!(verus_client::delta_ascending(&col));
    let falls: VerifiedDeltaColumn<u64> = verus_client::delta_built(&[1, 4, 3]);
    assert!(!verus_client::delta_ascending(&falls));
    let copy = verus_client::delta_copied_by_iter(&col);
    assert_eq!(verus_client::delta_values(&copy), values);
    assert_eq!(verus_client::delta_the_third(&col), Some(2));
    assert_eq!(verus_client::delta_the_third(&falls), Some(3));
    let short: VerifiedDeltaColumn<Option<i32>> = verus_client::delta_built(&[None, Some(-4)]);
    assert_eq!(verus_client::delta_the_third(&short), None);
    assert_eq!(
        verus_client::delta_read_in_a_loop(&short),
        vec![None, Some(-4)]
    );
    assert_eq!(
        verus_client::delta_in_the_range(&col, 3, 7),
        vec![5, 9, 9, 12]
    );
    assert_eq!(
        verus_client::delta_in_the_range(&col, 4, 4),
        Vec::<u64>::new()
    );
    assert_eq!(verus_client::delta_read_out(&col, 2, 6), (4, 6, 6));
    assert_eq!(verus_client::delta_read_out(&col, 7, 40), (3, 10, 10));
    assert_eq!(verus_client::delta_at_two_places(&col, 1, 4, 3), (2, 9, 2));
    verus_client::delta_where_it_stands(&col);
    verus_client::delta_moved_past(&col);
    verus_client::delta_told_by_the_call(&col);
    verus_client::delta_two_readers(&falls);

    let a: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[-3, 0, 7, 7, -20]);
    let b: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[-3, 0, 7, 8, -20]);
    assert!(verus_client::delta_agree(&a, &a));
    assert!(!verus_client::delta_agree(&a, &b));
    verus_client::delta_every_other(&a);

    // each call by itself
    let mut it = verus_client::delta_told_by_iter(&col);
    assert_eq!(verus_client::delta_told_by_next(&mut it), Some(1));
    assert_eq!(verus_client::delta_told_by_nth(&mut it, 2), Some(5));
    assert_eq!(verus_client::delta_told_by_an_iters_place(&it), 4);
    assert_eq!(verus_client::delta_told_by_an_iters_end(&it), 10);
    assert_eq!(verus_client::delta_told_by_an_iters_len(&it), 6);
    assert_eq!(verus_client::delta_told_by_a_size_hint(&it), (6, Some(6)));
    assert_eq!(verus_client::delta_window_len(&it), 6);
    verus_client::delta_told_by_the_source(&it, &col);
    verus_client::delta_told_by_set_max(&mut it, 8);
    assert_eq!(verus_client::delta_window_len(&it), 4);
    verus_client::delta_told_by_advance_to(&mut it, 5);
    verus_client::delta_told_by_advance_by(&mut it, 1);
    let mut copy = verus_client::delta_told_by_an_iters_clone(&it);
    assert_eq!(copy.next(), Some(12));
    assert_eq!(it.next(), Some(12));
    verus_client::delta_told_by_shift(&mut it, 8..10);
    assert_eq!(it.next(), Some(21));
    assert_eq!(
        verus_client::delta_told_by_shift_next(&mut it, 9..10),
        Some(30)
    );
    assert_eq!(verus_client::delta_told_by_next(&mut it), None);
    let mut it = verus_client::delta_told_by_iter_range(&col, 2, 5);
    assert_eq!(it.by_ref().collect::<Vec<u64>>(), vec![2, 5, 9]);
    let it = verus_client::delta_told_by_into_iter(&col);
    assert_eq!(it.len(), 10);
    let mut none: VerifiedDeltaIter<'_, u64> = verus_client::delta_told_by_an_iters_default();
    assert_eq!(none.next(), None);
}

#[test]
fn delta_runs_and_states() {
    use hexane::DeltaRun;
    let run = |prefix, delta, count| DeltaRun {
        prefix,
        delta,
        count,
    };
    let values: [u64; 4] = [5, 7, 9, 9];
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    assert_eq!(
        verus_client::delta_in_runs(&col),
        vec![run(0, Some(5), 1), run(5, Some(2), 2), run(9, Some(0), 1)]
    );
    let copy = verus_client::delta_copied_by_runs(&col);
    assert_eq!(verus_client::delta_values(&copy), values);
    let copy = verus_client::delta_copied_run_by_run(&col);
    assert_eq!(verus_client::delta_values(&copy), values);
    let mut dst: VerifiedDeltaColumn<u64> = verus_client::delta_built(&[1, 100]);
    verus_client::delta_moved_by_runs(&col, 1, 3, &mut dst, 1);
    assert_eq!(verus_client::delta_values(&dst), [1, 7, 9, 100]);
    verus_client::delta_runs_of_four(&col);
    let nulls: VerifiedDeltaColumn<Option<u32>> = verus_client::delta_built(&[None, None, Some(3)]);
    verus_client::delta_nulls_in_a_run(&nulls);
    assert_eq!(
        verus_client::delta_in_runs(&nulls),
        vec![run(0, None, 2), run(0, Some(3), 1)]
    );
    let signed: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[-3, 0, 7, 7, -20]);
    assert_eq!(
        verus_client::delta_the_number_before(&signed, 0),
        Some(run(0, Some(-3), 1))
    );
    assert_eq!(
        verus_client::delta_the_number_before(&signed, 4),
        Some(run(7, Some(-27), 1))
    );
    let mut it = col.iter();
    assert_eq!(
        verus_client::delta_two_runs_differ(&mut it),
        (Some(run(0, Some(5), 1)), Some(run(5, Some(2), 2)))
    );
    assert_eq!(
        verus_client::delta_two_runs_differ(&mut it),
        (Some(run(9, Some(0), 1)), None)
    );

    // a suspended iterator
    assert_eq!(verus_client::delta_read_in_two_goes(&col, 2), values);
    assert_eq!(verus_client::delta_read_in_two_goes(&col, 9), values);
    let it = verus_client::delta_resumed(&col, 1, 3).unwrap();
    assert_eq!(it.collect::<Vec<u64>>(), vec![7, 9]);
    verus_client::delta_resumed_at_the_end(&col, 1, 3);
    let mut edited: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    assert!(!verus_client::delta_ended_by_an_edit(&mut edited, 12));
    assert!(!verus_client::delta_ended_by_many(&mut edited, 12, 3));
    assert_eq!(
        verus_client::delta_values(&edited),
        [5, 7, 9, 9, 12, 12, 12, 12]
    );
    assert_eq!(verus_client::delta_a_copy_edited(&col, 1), (false, true));

    // each call by itself
    let mut it = col.iter_range(1, 4);
    assert_eq!(
        verus_client::delta_told_by_next_run(&mut it),
        Some(run(5, Some(2), 2))
    );
    let s = verus_client::delta_told_by_suspend(&it, &col);
    let mut runs = verus_client::delta_told_by_runs(it);
    assert_eq!(
        verus_client::delta_told_by_a_runs_next(&mut runs),
        Some(run(9, Some(0), 1))
    );
    assert_eq!(verus_client::delta_told_by_a_runs_next(&mut runs), None);
    let mut it = verus_client::delta_told_by_try_resume(&s, &col).unwrap();
    assert_eq!(it.pos(), 3);
    assert_eq!(it.next(), Some(9));
    assert_eq!(it.next(), None);
    // the state is a place in a copy of the column, which has not been
    // edited, and in no column that has been
    let same = col.clone();
    assert_eq!(
        verus_client::delta_resumed_elsewhere(&s, &same),
        Some(vec![9])
    );
    assert_eq!(verus_client::delta_resumed_elsewhere(&s, &edited), None);
    assert!(verus_client::delta_told_by_try_resume(&s, &edited).is_err());
}

#[test]
fn delta_scans_and_searches() {
    use std::ops::Bound;
    // values in no order: scans
    let values: [u64; 7] = [5, 3, 9, 3, 12, 3, 7];
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    assert_eq!(verus_client::delta_first_place_in(&col, 3, 0, 7), Some(1));
    assert_eq!(verus_client::delta_first_place_in(&col, 3, 2, 7), Some(3));
    assert_eq!(verus_client::delta_first_place_in(&col, 3, 6, 7), None);
    assert_eq!(verus_client::delta_all_places(&col, 3), [1, 3, 5]);
    assert!(verus_client::delta_all_places(&col, 4).is_empty());
    assert_eq!(verus_client::delta_scan_unfitting(&col), None);
    assert_eq!(
        verus_client::delta_first_in_bounds(&col, 8, 20),
        Some((2, 9))
    );
    assert_eq!(
        verus_client::delta_first_in_bounds(&col, 13, u64::MAX),
        None
    );
    let signed: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[-3, 0, 7, 7, -20]);
    assert_eq!(verus_client::delta_first_at_least(&signed, 1), Some((2, 7)));
    assert_eq!(verus_client::delta_first_at_least(&signed, 8), None);
    let small: VerifiedDeltaColumn<i32> = verus_client::delta_built(&[4, -1, 6, 5]);
    assert_eq!(
        verus_client::delta_first_between(&small, 4, 6),
        Some((2, 6))
    );
    assert_eq!(verus_client::delta_first_between(&small, 6, 4), None);
    let nullable: VerifiedDeltaColumn<Option<u32>> =
        verus_client::delta_built(&[None, Some(8), None, Some(2)]);
    assert_eq!(verus_client::delta_first_below(&nullable, 5), Some((3, 2)));
    assert_eq!(verus_client::delta_first_below(&nullable, 0), None);
    let numbers: VerifiedDeltaColumn<Option<i64>> =
        verus_client::delta_built(&[None, None, Some(-4), None]);
    assert_eq!(verus_client::delta_first_null(&numbers), Some(0));
    assert_eq!(
        verus_client::delta_any_number(&numbers),
        Some((2, Some(-4)))
    );
    let big: VerifiedDeltaColumn<Option<u64>> = verus_client::delta_built(&[None, Some(1), None]);
    assert_eq!(verus_client::delta_null_in_no_bounds(&big), None);

    // values in order: searches
    let sorted: [u64; 7] = [1, 3, 3, 3, 7, 9, 9];
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&sorted);
    assert_eq!(verus_client::delta_seek_then_read(&col, 3), 1..4);
    assert_eq!(verus_client::delta_seek_then_read(&col, 4), 4..4);
    assert_eq!(verus_client::delta_seek_then_read(&col, 0), 0..0);
    assert_eq!(verus_client::delta_seek_then_read(&col, 10), 7..7);
    assert_eq!(verus_client::delta_seek_unfitting(&col), 7..7);
    assert_eq!(verus_client::delta_sought_between(&col, 9, 2, 6), 5..6);
    assert_eq!(verus_client::delta_sought_between(&col, 1, 2, 6), 2..2);
    let mut it = col.iter_range(1, 6);
    assert_eq!(verus_client::delta_find_in_the_rest(&mut it, 7), 4..5);
    assert_eq!((it.pos(), it.end_pos()), (4, 6));
    assert_eq!(it.next(), Some(7));
    assert_eq!(verus_client::delta_find_in_the_rest(&mut it, 2), 5..5);
    assert_eq!(it.next(), Some(9));
    let first: VerifiedDeltaColumn<Option<u64>> =
        verus_client::delta_built(&[None, None, None, Some(0), Some(6)]);
    assert_eq!(verus_client::delta_seek_nulls_first(&first), 0..3);
    let mut it = col.iter();
    assert_eq!(
        verus_client::delta_sought_unfitting(&mut it, u64::MAX, 2..5),
        5..5
    );
    assert_eq!((it.pos(), it.end_pos(), it.next()), (5, 5, None));
    let mut it = col.iter();
    let (start, end) = (4, 1);
    assert_eq!(
        verus_client::delta_sought_backwards(&mut it, 3, start..end),
        4..4
    );
    assert_eq!((it.pos(), it.next()), (4, None));
    let mut it = col.iter();
    let (start, end) = (40, 1);
    assert_eq!(
        verus_client::delta_sought_backwards(&mut it, 3, start..end),
        7..7
    );

    // each call by itself
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    let mut it = col.iter();
    assert_eq!(
        verus_client::delta_told_by_scan_to_value(&mut it, 3),
        Some(1)
    );
    assert_eq!(
        verus_client::delta_told_by_scan_to_value(&mut it, 3),
        Some(3)
    );
    assert_eq!(
        verus_client::delta_told_by_scan_to_range(&mut it, 10..),
        Some((4, 12))
    );
    let bounds = (Bound::Excluded(3), Bound::Included(7));
    assert_eq!(
        verus_client::delta_told_by_scan_to_range(&mut it, bounds),
        Some((6, 7))
    );
    assert_eq!(verus_client::delta_told_by_scan_to_range(&mut it, ..), None);
    assert_eq!(verus_client::delta_told_by_scan_to_value(&mut it, 3), None);
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&sorted);
    let mut it = col.iter();
    assert_eq!(
        verus_client::delta_told_by_seek_to_value(&mut it, 9, 0..usize::MAX),
        5..7
    );
    assert_eq!(it.next(), Some(9));
    assert_eq!(
        verus_client::delta_told_by_scope_to_value(&col, 3, 0, 7),
        1..4
    );
    assert_eq!(
        verus_client::delta_told_by_scope_to_value(&col, 5, 2, 7),
        4..4
    );
}

#[test]
fn delta_finds() {
    use hexane::verified::VerifiedFindByRange;
    let values: [u64; 7] = [5, 3, 9, 3, 12, 3, 7];
    let col: VerifiedDeltaColumn<u64> = verus_client::delta_built(&values);
    assert_eq!(verus_client::delta_found_all(&col, 3), [1, 3, 5]);
    assert!(verus_client::delta_found_all(&col, 4).is_empty());
    assert_eq!(verus_client::delta_found_between(&col, 5, 12), [0, 2, 6]);
    assert!(verus_client::delta_found_between(&col, 12, 5).is_empty());
    assert_eq!(verus_client::delta_found_twice(&col, 3), (Some(1), Some(3)));
    assert_eq!(verus_client::delta_found_twice(&col, 12), (Some(4), None));
    assert_eq!(verus_client::delta_found_twice(&col, 0), (None, None));
    assert_eq!(verus_client::delta_find_unfitting(&col), None);
    let small: VerifiedDeltaColumn<Option<i32>> =
        verus_client::delta_built(&[None, Some(8), None, Some(2), Some(-1)]);
    assert_eq!(verus_client::delta_found_at_most(&small, 5), Some(3));
    assert_eq!(verus_client::delta_found_at_most(&small, -2), None);
    let nullable: VerifiedDeltaColumn<Option<u64>> =
        verus_client::delta_built(&[None, Some(1), None]);
    assert_eq!(verus_client::delta_find_null(&nullable), (None, None));
    let signed: VerifiedDeltaColumn<i64> = verus_client::delta_built(&[-3, 0, 7, 7, -20]);
    assert_eq!(verus_client::delta_find_first_number(&signed, 7), Some(2));
    assert_eq!(verus_client::delta_find_first_number(&signed, 8), None);
    assert_eq!(verus_client::delta_find_nothing(), None);

    // each call by itself
    let finder = verus_client::delta_told_by_find_by_range(&col, 4..=9);
    assert_eq!(finder.collect::<Vec<_>>(), [0, 2, 6]);
    let mut finder = verus_client::delta_told_by_find_by_value(&col, 3);
    assert_eq!(verus_client::delta_told_by_find_next(&mut finder), Some(1));
    assert_eq!(finder.collect::<Vec<_>>(), [3, 5]);
    assert_eq!(verus_client::delta_told_by_find_first(&col, 9), Some(2));
    assert_eq!(verus_client::delta_told_by_find_first(&col, 8), None);
    let mut none: VerifiedFindByRange<'static, u64> = verus_client::delta_told_by_find_default();
    assert_eq!(none.next(), None);
}

#[test]
fn delta_encoders() {
    use hexane::verified::{VerifiedDeltaEncoder, VerifiedDeltaEncoderState};
    let input: Vec<Option<i64>> = vec![Some(4), Some(4), None, Some(-9), Some(i64::MAX - 20)];
    let col: VerifiedDeltaColumn<Option<i64>> = verus_client::delta_built(&input);
    let bytes = col.save();

    // one encoding, whichever way it is made
    assert_eq!(
        verus_client::delta_encoded_as_saved::<_, Leb128>(&input),
        bytes
    );
    verus_client::delta_encoder_or_column::<_, Leb128>(&input);
    let back = verus_client::delta_encode_then_load::<_, Leb128>(&input).unwrap();
    assert_eq!(back.to_vec(), input);
    assert_eq!(verus_client::delta_reencoded(&col), bytes);
    assert_eq!(verus_client::delta_reencoded_in_a_loop(&col), bytes);
    assert_eq!(
        verus_client::delta_one_encoding_four_ways::<_, Leb128>(input.clone()),
        bytes
    );
    let mut buf = vec![7, 7];
    let r = verus_client::delta_streamed_as_saved::<_, Leb128>(&input, &mut buf);
    assert_eq!(r, 2..2 + bytes.len());
    assert_eq!(buf[r], bytes[..]);
    assert_eq!(
        verus_client::delta_told_by_encode::<_, Leb128>(input.clone()),
        bytes
    );
    let r = verus_client::delta_told_by_encode_to::<_, Leb128>(&mut buf, input.clone());
    assert_eq!(buf[r], bytes[..]);
    let n = buf.len();
    let r = verus_client::delta_told_by_encode_to_unless::<_, Leb128>(
        &mut buf,
        vec![Some(6i64); 3],
        Some(6),
    );
    assert_eq!((r, buf.len()), (n..n, n));
    let r =
        verus_client::delta_told_by_encode_to_unless::<_, Leb128>(&mut buf, input.clone(), Some(4));
    assert_eq!(buf[r], bytes[..]);

    // any iterator of a type whose every value fits; and one of numbers
    // that may not, checked as they are given
    let small: VerifiedDeltaColumn<u32> = verus_client::delta_built(&[3, 4, 5]);
    assert_eq!(
        verus_client::delta_encode_any_iterator([3u32, 4, 5].into_iter()),
        small.save()
    );
    let misfit = vec![Some(1), None, Some(u64::MAX)];
    assert_eq!(verus_client::delta_encode_checked(&misfit), None);
    let fit = vec![Some(1), None, Some(i64::MAX as u64)];
    let wide: VerifiedDeltaColumn<Option<u64>> = verus_client::delta_built(&fit);
    assert_eq!(verus_client::delta_encode_checked(&fit), Some(wide.save()));

    // each call of the encoder by itself
    let (mut enc, other): (VerifiedDeltaEncoder<'_, Option<i64>>, _) =
        verus_client::delta_told_by_a_new_encoder();
    assert_eq!(
        verus_client::delta_told_by_an_encoders_count(&other),
        (0, true)
    );
    assert!(verus_client::delta_told_by_an_encoders_save(other).is_empty());
    verus_client::delta_told_by_an_encoders_appends(&mut enc, Some(4), Some(4), 0);
    verus_client::delta_told_by_an_encoders_extend(&mut enc, vec![None]);
    verus_client::delta_told_by_an_extend_of_any(&mut enc, input[3..].iter().copied());
    assert_eq!(
        verus_client::delta_told_by_an_encoders_count(&enc),
        (5, false)
    );
    let mut out = vec![1];
    let r = verus_client::delta_told_by_an_encoders_save_to(enc, &mut out);
    assert_eq!((r.clone(), &out[r]), (1..1 + bytes.len(), &bytes[..]));
    let mut ones = VerifiedDeltaEncoder::<i64>::new();
    ones.append_n(1, 5);
    let r = verus_client::delta_told_by_an_encoders_save_to_unless(ones, &mut out, 1);
    assert_eq!(
        (r, out.len()),
        (1 + bytes.len()..1 + bytes.len(), 1 + bytes.len())
    );
    let mut ones = VerifiedDeltaEncoder::<u64>::new();
    ones.append_n(1, 5);
    let mut out = vec![];
    verus_client::delta_unless_a_misfit(ones, &mut out, u64::MAX);
    assert_eq!(out, VerifiedDeltaEncoder::<u64>::encode([1; 5]));
    let mut far = VerifiedDeltaEncoder::<i64>::new();
    far.extend([i64::MIN, -1]);
    let saved = verus_client::delta_saved_within_a_span(far, 0, 1);
    assert_eq!(saved, VerifiedDeltaEncoder::<i64>::encode([i64::MIN, -1]));

    // and of the state that writes into its caller's buffer
    let (mut state, other): (VerifiedDeltaEncoderState<'_, Option<i64>>, _) =
        verus_client::delta_told_by_a_new_state();
    let mut buf = vec![9];
    assert_eq!(
        verus_client::delta_told_by_a_states_count(&other, &buf),
        (0, true)
    );
    verus_client::delta_told_by_a_finish(other, &mut buf);
    assert_eq!(buf, [9]);
    verus_client::delta_told_by_a_states_append(&mut state, &mut buf, Some(4));
    assert!(verus_client::delta_told_by_is_uniform(
        &state,
        &buf,
        Some(4)
    ));
    verus_client::delta_told_by_a_states_append_n(&mut state, &mut buf, Some(4), 1);
    verus_client::delta_told_by_a_states_extend(&mut state, &mut buf, input[2..].to_vec());
    assert!(!verus_client::delta_told_by_is_uniform(
        &state,
        &buf,
        Some(4)
    ));
    assert_eq!(
        verus_client::delta_told_by_a_states_count(&state, &buf),
        (5, false)
    );
    verus_client::delta_told_by_a_finish(state, &mut buf);
    assert_eq!((buf[0], &buf[1..]), (9, &bytes[..]));

    // two states into one buffer, and a state's word on its one value
    let mut buf = vec![1];
    let mid = verus_client::delta_two_streamed(&mut buf);
    let first: VerifiedDeltaColumn<u32> = verus_client::delta_built(&[7, 7, 9]);
    let second: VerifiedDeltaColumn<Option<i64>> =
        verus_client::delta_built(&[Some(-5), None, None, Some(i64::MIN)]);
    assert_eq!(buf[1..mid], first.save()[..]);
    assert_eq!(buf[mid..], second.save()[..]);
    assert_eq!(
        verus_client::delta_uniform_until_another(&mut vec![]),
        (true, true, false, false, 4)
    );
}

/// The client that proves a save of numbers 2^63 apart never returns:
/// it panics instead.
#[test]
#[should_panic(expected = "delta values out of domain: they span 2^63 or more")]
fn delta_encoder_stops_at_a_span_too_wide() {
    verus_client::delta_too_wide_never_saved();
}

#[test]
fn delta_decoders() {
    use hexane::verified::VerifiedDeltaDecoder;
    fn run(count: usize, value: Option<i64>) -> Option<Run<Option<i64>>> {
        Some(Run { count, value })
    }
    // the differences are 4, 4, 4, two nulls, -21 and `i64::MAX - 11`
    let last = i64::MAX - 20;
    let input = vec![Some(4), Some(8), Some(12), None, None, Some(-9), Some(last)];
    let col: VerifiedDeltaColumn<Option<i64>> = verus_client::delta_built(&input);
    let bytes = col.save();

    // the values of a saved column, whichever way they are read
    let dec = VerifiedDeltaDecoder::<Option<i64>>::new(&bytes);
    assert_eq!(verus_client::delta_read_all(dec), input);
    assert_eq!(
        verus_client::delta_decode_all::<Option<i64>, Leb128>(&bytes),
        input
    );
    assert_eq!(verus_client::delta_decoded_as_held(&col), input);
    assert_eq!(
        verus_client::delta_encoded_then_decoded::<_, Leb128>(&input),
        input
    );
    assert_eq!(
        verus_client::delta_decode_unknown::<Option<i64>, Leb128>(&bytes),
        Some(input.clone())
    );
    assert_eq!(
        verus_client::delta_decode_unknown::<u32, Leb128>(&bytes),
        None
    );
    let (loaded, read) =
        verus_client::delta_decoded_or_loaded::<Option<i64>, Leb128>(&bytes).unwrap();
    assert_eq!((loaded.to_vec(), read), (input.clone(), input.clone()));
    assert!(verus_client::delta_decoded_or_loaded::<i64, Leb128>(&bytes).is_none());

    // each call of the decoder by itself
    let mut dec: VerifiedDeltaDecoder<'_, Option<i64>> =
        verus_client::delta_told_by_a_decoder(&bytes);
    assert!(verus_client::delta_told_by_a_checked_decoder::<u32, Leb128>(&bytes).is_err());
    let mut other =
        verus_client::delta_told_by_a_checked_decoder::<Option<i64>, Leb128>(&bytes).unwrap();
    assert_eq!(
        verus_client::delta_told_by_a_decoders_next(&mut dec),
        Some(Some(4))
    );
    assert_eq!(
        verus_client::delta_told_by_a_decoders_nth(&mut dec, 1),
        Some(Some(12))
    );
    verus_client::delta_told_by_a_decoders_advance(&mut dec, 1);
    let copy = verus_client::delta_told_by_a_decoders_copy(&dec);
    assert_eq!(copy.collect::<Vec<_>>(), input[4..]);
    assert_eq!(verus_client::delta_peek_ahead(&dec, 1), Some(Some(-9)));
    assert_eq!(verus_client::delta_peek_ahead(&dec, 3), None);
    assert_eq!(
        verus_client::delta_told_by_a_decoders_nth(&mut dec, 9),
        None
    );
    assert_eq!(verus_client::delta_told_by_a_decoders_next(&mut dec), None);

    // runs of differences: of one difference, no longer than asked for,
    // each standing for the values it passes over
    assert_eq!(
        verus_client::delta_told_by_a_decoders_run(&mut other, 2),
        run(2, Some(4))
    );
    assert_eq!(
        verus_client::delta_told_by_a_decoders_run(&mut other, 0),
        None
    );
    assert_eq!(
        verus_client::delta_run_stands_for(&mut other, 9),
        run(1, Some(4))
    );
    assert_eq!(
        verus_client::delta_run_then_next(&mut other, 9),
        Some((2, Some(-9)))
    );
    assert_eq!(
        verus_client::delta_run_stands_for(&mut other, 9),
        run(1, Some(i64::MAX - 11))
    );
    assert_eq!(
        verus_client::delta_told_by_a_decoders_run(&mut other, 9),
        None
    );
    assert_eq!(verus_client::delta_run_then_next(&mut other, 9), None);

    // a decoder's first run, given to a column
    let first: VerifiedDeltaColumn<Option<i64>> = verus_client::delta_first_run_rebuilt(&bytes);
    assert_eq!(first.to_vec(), input[..3]);
    let none: VerifiedDeltaColumn<u32> = verus_client::delta_first_run_rebuilt(&[]);
    assert!(none.to_vec().is_empty());
}

#[test]
fn raw_columns() {
    use hexane::verified::{RawColumn, RawColumnIter};
    let text = b"goodbye, world";
    let mut col: RawColumn = verus_client::raw_built(b"hello, ", b"world");
    assert_eq!(col.save(), b"hello, world");
    verus_client::raw_replaced(&mut col, 0, 5, b"goodbye");
    assert_eq!(col.save(), text);
    verus_client::raw_cut_and_pasted(&mut col, 3, 9);
    assert_eq!(col.save(), text);
    assert_eq!(verus_client::raw_round_trip(b"any bytes"), b"any bytes");

    // a column loaded from what another saved; the two in one buffer
    let twin: RawColumn = verus_client::raw_saved_then_loaded(&col);
    assert_eq!(twin.save(), text);
    let (buf, first, second) = verus_client::raw_two_in_one_buffer(&col, &twin);
    assert_eq!((&buf[first], &buf[second]), (&text[..], &text[..]));

    // reads of a range, and by an iterator
    assert_eq!(verus_client::raw_read(&twin, 3, 7), Some(&b"dbye"[..]));
    assert_eq!(verus_client::raw_read(&twin, 3, 3), Some(&b""[..]));
    assert_eq!(verus_client::raw_read(&twin, 3, 99), None);
    let both = verus_client::raw_read_again(&twin, 0, 7).unwrap();
    assert_eq!(both, (&b"goodbye"[..], &b"goodbye"[..]));
    assert!(verus_client::raw_read_again(&twin, 9, 99).is_none());
    let (x, y) = verus_client::raw_two_values(&twin, 7, 2);
    assert_eq!((x, y), (&b"goodbye"[..], &b", "[..]));
    assert_eq!(verus_client::raw_taken_at(&twin, 9, 5), b"world");
    assert_eq!(verus_client::raw_skipped(&twin, 9, 5), (&b"world"[..], 14));
    assert_eq!(verus_client::raw_sought(&twin, 2, 9, 5), b"world");
    let (x, y) = verus_client::raw_read_twice(&twin, 0, 7);
    assert_eq!((x, y), (&b"goodbye"[..], &b"goodbye"[..]));

    // a clone is a column of its own
    let more = verus_client::raw_clone_then_diverge(&twin, b"!");
    assert_eq!(more.save(), b"goodbye, world!");
    assert_eq!(twin.save(), text);

    // each call by itself
    let (new, with, default) = verus_client::raw_told_by_new(8);
    assert!(new.is_empty() && with.is_empty() && default.is_empty());
    assert_eq!(verus_client::raw_told_by_len(&twin), (14, false));
    let mut other = verus_client::raw_told_by_clone(&twin);
    assert!(verus_client::raw_told_by_try_splice_slice(&mut other, 15, 0, b"x").is_err());
    assert!(verus_client::raw_told_by_try_splice_slice(&mut other, 14, 0, b"?").is_ok());
    verus_client::raw_told_by_splice_slice(&mut other, 0, 7, b"hello");
    assert_eq!(other.save(), b"hello, world?");
    assert_eq!(
        verus_client::raw_told_by_try_get(&twin, 9..14).unwrap(),
        b"world"
    );
    assert!(verus_client::raw_told_by_try_get(&twin, 9..15).is_err());
    assert_eq!(verus_client::raw_told_by_get(&twin, 0..7), b"goodbye");
    let mut out = vec![1u8];
    let (saved, range) = verus_client::raw_told_by_save(&twin, &mut out);
    assert_eq!(
        (&saved[..], range, &out[1..]),
        (&text[..], 1..15, &text[..])
    );
    let (plain, small) = verus_client::raw_told_by_load(b"abc", 2);
    assert_eq!(plain.unwrap().save(), b"abc");
    assert_eq!(small.unwrap().save(), b"abc");

    // an iterator's calls
    let (start, mut it) = verus_client::raw_told_by_iter(&twin, 9);
    assert_eq!((start.pos(), it.pos()), (0, 9));
    assert_eq!(verus_client::raw_told_by_take(&mut it, 2), (9, &b"wo"[..]));
    verus_client::raw_told_by_skip(&mut it, 1);
    assert_eq!(it.pos(), 12);
    verus_client::raw_told_by_seek_to(&mut it, 13);
    let (mut same, none): (RawColumnIter<'_>, RawColumnIter<'_>) =
        verus_client::raw_told_by_an_iterators_clone(&it);
    assert_eq!((same.pos(), none.pos()), (13, 0));
    assert_eq!((same.take(1), it.take(1)), (&b"d"[..], &b"d"[..]));

    verus_client::raw_emptied(&mut col);
    assert!(col.is_empty());
}

#[test]
fn raw_cuts() {
    use hexane::verified::RawColumn;
    let mut col = RawColumn::with_max_segments(2);
    assert_eq!(
        verus_client::raw_put_then_read(&mut col, 0, b"world"),
        b"world"
    );
    assert_eq!(
        verus_client::raw_put_then_read(&mut col, 0, b"hello, "),
        b"hello, "
    );
    assert_eq!(col.save(), b"hello, world");
    let (two, a, b) = verus_client::raw_two_values_put(b"left", b"right");
    assert_eq!(two.save(), b"leftright");
    assert_eq!((&a[..], &b[..]), (&b"left"[..], &b"right"[..]));
    assert_eq!(
        verus_client::raw_loaded_value_read(b"any bytes"),
        b"any bytes"
    );
    let (x, y) = verus_client::raw_clone_reads_alike(&col, 7, 12);
    assert_eq!((&x[..], &y[..]), (&b"world"[..], &b"world"[..]));

    // a column that holds values, their lengths kept beside it: each is
    // read whole, however small the slabs
    let mut vals = RawColumn::with_max_segments(1);
    let mut lens: Vec<usize> = Vec::new();
    verus_client::raw_value_put(&mut vals, &mut lens, 0, 0, b"bb");
    verus_client::raw_value_put(&mut vals, &mut lens, 0, 0, b"a");
    verus_client::raw_value_put(&mut vals, &mut lens, 2, 3, b"dddd");
    verus_client::raw_value_put(&mut vals, &mut lens, 2, 3, b"");
    verus_client::raw_value_put(&mut vals, &mut lens, 2, 3, b"ccc");
    assert_eq!(vals.save(), b"abbcccdddd");
    assert_eq!(lens, [1, 2, 3, 0, 4]);
    let wanted: [&[u8]; 5] = [b"a", b"bb", b"ccc", b"", b"dddd"];
    let mut start = 0;
    for (i, want) in wanted.into_iter().enumerate() {
        assert_eq!(verus_client::raw_value_read(&vals, &lens, i, start), want);
        start += lens[i];
    }
    verus_client::raw_value_taken_out(&mut vals, &mut lens, 1, 1);
    assert_eq!(vals.save(), b"acccdddd");
    assert_eq!(lens, [1, 3, 0, 4]);
    assert_eq!(verus_client::raw_value_read(&vals, &lens, 1, 1), b"ccc");
    assert_eq!(verus_client::raw_value_read(&vals, &lens, 3, 4), b"dddd");

    // each call by itself
    let (new, with, default) = verus_client::raw_cuts_told_by_new(8);
    assert!(new.is_empty() && with.is_empty() && default.is_empty());
    let (plain, small) = verus_client::raw_cuts_told_by_load(b"abc", 2);
    assert!(plain.try_get(0..3).is_ok() && small.try_get(0..3).is_ok());
    let mut other = verus_client::raw_cuts_told_by_clone(&col);
    assert!(verus_client::raw_cuts_told_by_try_splice_slice(&mut other, 13, 0, b"x").is_err());
    assert!(verus_client::raw_cuts_told_by_try_splice_slice(&mut other, 12, 0, b"!").is_ok());
    verus_client::raw_cuts_told_by_splice_slice(&mut other, 0, 5, b"goodbye");
    assert_eq!(other.save(), b"goodbye, world!");
    verus_client::raw_told_by_the_cuts(&other, 0, 7);
}

// ── The codecs ───────────────────────────────────────────────

/// Numbers at the ends of the encodings' lengths, and two more.
const CODEC_NUMBERS: [(u64, i64); 6] = [
    (0, 0),
    (127, -64),
    (247, -124),
    (248, 124),
    (1 << 40, i64::MAX),
    (u64::MAX, i64::MIN),
];

#[test]
fn codecs() {
    for (n, i) in CODEC_NUMBERS {
        verus_client::codecs_leb128(n, i, b"rest", &[0x80, 0x80, 0x01]);
        verus_client::codecs_leb128(n, i, b"", &[0x80]);
        assert_eq!(
            verus_client::a_number_written_and_read::<Leb128>(n, b"x"),
            n
        );
        assert_eq!(
            verus_client::a_signed_number_written_and_read::<Leb128>(i, b""),
            i
        );
        assert_eq!(
            verus_client::numbers_one_after_another::<Leb128>(n, i),
            (n, i)
        );
        assert_eq!(verus_client::a_number_skipped::<Leb128>(i, n), n);
        let mut buf = b"ab".to_vec();
        verus_client::numbers_take_one_to_ten_bytes::<Leb128>(&mut buf, n, i);
        assert!((4..=22).contains(&buf.len()));
    }
    for data in [&[][..], &[0x80], &[0x80, 0x00], &[0xff; 11], &[0x05, 0x06]] {
        assert!(verus_client::readers_agree::<Leb128>(data));
    }
    let (col, bytes) = verus_client::leb128_by_its_import(7, 300);
    assert_eq!(col.to_vec(), [7, 300]);
    assert_eq!(bytes, [0x7e, 0x07, 0xac, 0x02]);
}

#[cfg(feature = "bijou64")]
#[test]
fn bijou64() {
    use hexane::verified::bijou;
    use hexane::Bijou64;
    for (n, i) in CODEC_NUMBERS {
        verus_client::codecs_bijou64(n, i, b"rest", &[0xf9, 0x00]);
        verus_client::codecs_bijou64(n, i, b"", &[0xff; 9]);
        assert_eq!(
            verus_client::a_number_written_and_read::<Bijou64>(n, b"x"),
            n
        );
        assert_eq!(
            verus_client::a_signed_number_written_and_read::<Bijou64>(i, b""),
            i
        );
        assert_eq!(
            verus_client::numbers_one_after_another::<Bijou64>(n, i),
            (n, i)
        );
        assert_eq!(verus_client::a_number_skipped::<Bijou64>(i, n), n);
        let mut buf = b"ab".to_vec();
        verus_client::numbers_take_one_to_ten_bytes::<Bijou64>(&mut buf, n, i);
        assert!((4..=22).contains(&buf.len()));
    }
    for data in [&[][..], &[0xf8], &[0xf8, 0x00], &[0xff; 9], &[0x05, 0x06]] {
        assert!(verus_client::readers_agree::<Bijou64>(data));
    }

    let input: Vec<u64> = (0..1000).map(|i| i / 3 * 100).collect();
    let nullable = vec![Some(-1), None, None, Some(i64::MIN)];
    let flags = vec![true, false, false];
    let actions = vec![
        Some(Action::Make),
        None,
        Some(Action::Set),
        Some(Action::Set),
    ];
    let bytes = bijou::VerifiedColumn::<u64>::from_values(&input).save();
    verus_client::columns_with_bijou64(
        &input,
        &nullable,
        &flags,
        &actions,
        flags.clone(),
        nullable.clone(),
        &bytes,
    );
    let acts = bijou::VerifiedColumn::<Option<Action>>::from_values(&actions).save();
    verus_client::columns_with_bijou64(&vec![], &vec![], &vec![], &vec![], vec![], vec![], &acts);
    verus_client::strings_with_bijou64(&vec![&b"ab"[..], b"", b"ab"], &vec!["é", "", "x"]);

    let sums = bijou::VerifiedPrefixColumn::<u64>::from_values(&[3, 0, 4]).save();
    verus_client::sums_with_bijou64(&sums, 3);
    verus_client::sums_with_bijou64(&[0xf9, 0x00], 0);

    let values: Vec<i64> = vec![10, 11, 12, -500, i64::MAX >> 2];
    let some = vec![Some(5u32), None, Some(u32::MAX), Some(0)];
    let saved = bijou::VerifiedDeltaColumn::<i64>::from_values(&values).save();
    verus_client::deltas_with_bijou64(&values, &some, values.clone(), &saved);
    verus_client::deltas_with_bijou64(&vec![], &vec![], vec![], &[0xff; 9]);

    // the codec named by an import: the same values, other bytes
    let (col, bytes) = verus_client::bijou64_by_its_import(7, 300);
    assert_eq!(col.to_vec(), [7, 300]);
    assert_eq!(bytes, [0x03, 0x07, 0xf8, 0x34]);
    let (leb, leb_bytes) = verus_client::leb128_by_its_import(7, 300);
    assert_eq!(leb.to_vec(), col.to_vec());
    assert_ne!(leb_bytes, bytes);

    let old = VerifiedColumn::<Option<i64>>::from_values(&nullable);
    let new = verus_client::from_one_codec_to_the_other(&old);
    assert_eq!(new.to_vec(), nullable);
    assert_eq!(
        new.save(),
        bijou::VerifiedColumn::<Option<i64>>::from_values(&nullable).save()
    );
    let old = VerifiedDeltaColumn::<i64>::from_values(&values);
    let new = verus_client::deltas_from_one_codec_to_the_other(&old);
    assert_eq!(new.to_vec(), values);
    assert_eq!(new.save(), saved);

    assert_eq!(verus_client::streamed_with_bijou64(7, 300), [7, 300]);
    assert_eq!(
        verus_client::deltas_streamed_with_bijou64(-7, 300),
        [-7, 300]
    );
}

// ── The edit cursor ──────────────────────────────────────────

/// A value as the cursor's clients take it, and as these tests compare
/// it.
trait Lent<C: VCodec>: VValue<C> + Clone {
    type Seen: Clone + PartialEq + std::fmt::Debug;

    fn lend(&self) -> Get<'_, Self>;

    fn seen(v: Get<'_, Self>) -> Self::Seen;
}

impl<C: VCodec> Lent<C> for u64 {
    type Seen = u64;

    fn lend(&self) -> u64 {
        *self
    }

    fn seen(v: u64) -> u64 {
        v
    }
}

impl<C: VCodec> Lent<C> for Option<i64> {
    type Seen = Option<i64>;

    fn lend(&self) -> Option<i64> {
        *self
    }

    fn seen(v: Option<i64>) -> Option<i64> {
        v
    }
}

impl<C: VCodec> Lent<C> for bool {
    type Seen = bool;

    fn lend(&self) -> bool {
        *self
    }

    fn seen(v: bool) -> bool {
        v
    }
}

impl<C: VCodec> Lent<C> for Vec<u8> {
    type Seen = Vec<u8>;

    fn lend(&self) -> &[u8] {
        self
    }

    fn seen(v: &[u8]) -> Vec<u8> {
        v.to_vec()
    }
}

impl<C: VCodec> Lent<C> for String {
    type Seen = String;

    fn lend(&self) -> &str {
        self
    }

    fn seen(v: &str) -> String {
        v.to_string()
    }
}

impl<C: VCodec> Lent<C> for Mark {
    type Seen = u64;

    fn lend(&self) -> Mark {
        *self
    }

    fn seen(v: Mark) -> u64 {
        v.0
    }
}

/// What a column holds.
fn seen<V: Lent<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>) -> Vec<V::Seen> {
    col.to_vec().into_iter().map(V::seen).collect()
}

/// The cursor's clients for every kind of column, on a column of
/// `model`, which has four values or more, with two values of the
/// caller's.
fn cursor_clients<V: Lent<C>, C: VCodec, K: VIndex<V, C>>(model: &[V], x: &V, y: &V) {
    let n = model.len();
    assert!(n >= 4);
    let lent: Vec<Get<'_, V>> = model.iter().map(V::lend).collect();
    let fresh = || VerifiedColumn::<V, C, K>::from_values(&lent);
    let was: Vec<V::Seen> = model.iter().map(|v| V::seen(v.lend())).collect();
    let (sx, sy) = (V::seen(x.lend()), V::seen(y.lend()));

    for (at, del) in [(0, 0), (1, 2), (n, 0), (0, n), (n - 1, 1)] {
        let mut col = fresh();
        verus_client::edit_like_a_splice(&mut col, at, del, x.lend());
        let mut want = was.clone();
        want.splice(at..at + del, [sx.clone()]);
        assert_eq!(seen(&col), want);
    }

    for at in [0, n - 1] {
        let mut col = fresh();
        let places = verus_client::places_of_a_cursor(&mut col, at, y.lend());
        assert_eq!(places, (at, at, at + 1, at + 2));
        let mut want = was.clone();
        want.splice(at..at + 1, [sy.clone(), sy.clone()]);
        assert_eq!(seen(&col), want);
    }

    for (a, b) in [(0, 0), (1, 3), (0, n), (n, n)] {
        let mut col = fresh();
        verus_client::two_places_one_edit(&mut col, a, b, x.lend(), y.lend());
        let mut want = was[..a].to_vec();
        want.push(sx.clone());
        want.extend_from_slice(&was[a..b]);
        want.push(sy.clone());
        want.extend_from_slice(&was[b..]);
        assert_eq!(seen(&col), want);
    }

    for (a, b, k) in [
        (0, 0, 0),
        (1, 2, 1),
        (2, usize::MAX, 5),
        (n, n + 3, usize::MAX),
    ] {
        let mut col = fresh();
        verus_client::only_passed_over(&mut col, a, b, k);
        assert_eq!(seen(&col), was);
    }

    let mut col = fresh();
    verus_client::pushed_by_a_cursor(&mut col, x.lend());
    let mut want = was.clone();
    want.push(sx.clone());
    assert_eq!(seen(&col), want);

    for at in [0, 2, n, n + 5, usize::MAX] {
        let mut col = fresh();
        verus_client::cut_short(&mut col, at);
        assert_eq!(seen(&col), was[..at.min(n)]);
    }

    for (at, k) in [(0, 0), (2, 3), (n, 1)] {
        let mut col = fresh();
        verus_client::runs_by_a_cursor(&mut col, at, x.lend(), y.lend(), k);
        let mut want = was[..at].to_vec();
        want.extend(std::iter::repeat_n(sx.clone(), k));
        want.extend([sy.clone(), sy.clone(), sx.clone()]);
        want.extend_from_slice(&was[at..]);
        assert_eq!(seen(&col), want);

        let mut col = fresh();
        let runs = vec![
            Run {
                count: 2,
                value: x.lend(),
            },
            Run {
                count: 0,
                value: y.lend(),
            },
            Run {
                count: k,
                value: y.lend(),
            },
        ];
        verus_client::any_runs_by_a_cursor(&mut col, at, runs.into_iter());
        let mut want = was[..at].to_vec();
        want.extend([sx.clone(), sx.clone()]);
        want.extend(std::iter::repeat_n(sy.clone(), k));
        want.extend_from_slice(&was[at..]);
        assert_eq!(seen(&col), want);

        let mut col = fresh();
        let none: Vec<Run<Get<'_, V>>> = Vec::new();
        verus_client::any_runs_by_a_cursor(&mut col, at, none.into_iter());
        assert_eq!(seen(&col), was);
    }

    for at in 0..=n.min(6) {
        let mut col = fresh();
        let there = verus_client::is_it_there(&mut col, at, model[1].lend());
        assert_eq!(there, at < n && was[at] == was[1]);
        assert!(!verus_client::is_it_there(&mut col, n, x.lend()));
        assert_eq!(seen(&col), was);
    }

    for at in [0, 1, n - 1] {
        let mut col = fresh();
        verus_client::put_in_its_place(&mut col, at, x.lend());
        let mut want = was.clone();
        want[at] = sx.clone();
        assert_eq!(seen(&col), want);
        verus_client::put_in_its_place(&mut col, at, x.lend());
        assert_eq!(seen(&col), want);
    }

    // each step's contract, asked for on its own
    let mut col = fresh();
    let mut at = 0;
    let mut out: Vec<V::Seen> = Vec::new();
    {
        let mut e = verus_client::told_by_an_edit(&mut col);
        assert_eq!(verus_client::told_by_a_cursors_places(&e), (0, 0));
        verus_client::told_by_a_cursors_advance(&mut e, 1);
        out.extend_from_slice(&was[at..at + 1]);
        at += 1;
        verus_client::told_by_a_cursors_seek(&mut e, 2);
        out.extend_from_slice(&was[at..2]);
        at = 2;
        verus_client::told_by_a_cursors_insert(&mut e, x.lend());
        out.push(sx.clone());
        verus_client::told_by_a_cursors_insert_run(&mut e, y.lend(), 3);
        out.extend([sy.clone(), sy.clone(), sy.clone()]);
        let runs = vec![Run {
            count: 2,
            value: x.lend(),
        }];
        verus_client::told_by_a_cursors_insert_runs(&mut e, runs.into_iter());
        out.extend([sx.clone(), sx.clone()]);
        assert_eq!(
            verus_client::told_by_a_cursors_peek(&mut e, x.lend()),
            Some(was[at] == sx)
        );
        verus_client::told_by_a_cursors_replace_with(&mut e, y.lend());
        out.push(sy.clone());
        at += 1;
        verus_client::told_by_a_cursors_delete(&mut e, 1);
        at += 1;
        // an advance, an insertion, a deletion, a run and a replacement
        verus_client::told_what_a_step_keeps(&mut e, 1, x.lend());
        let k = 1.min(n - at);
        out.extend_from_slice(&was[at..at + k]);
        at += k;
        out.push(sx.clone());
        at += 1.min(n - at);
        out.push(sx.clone());
        if at < n {
            out.push(sx.clone());
            at += 1;
        }
        assert_eq!(verus_client::told_by_a_cursors_places(&e), (at, out.len()));
        verus_client::told_by_a_cursors_finish(e);
    }
    out.extend_from_slice(&was[at..]);
    assert_eq!(seen(&col), out);

    // and at the end of a column, where a step finds nothing
    let mut col = fresh();
    {
        let mut e = verus_client::told_by_an_edit_at(&mut col, n);
        assert_eq!(verus_client::told_by_a_cursors_places(&e), (n, n));
        assert_eq!(verus_client::told_by_a_cursors_peek(&mut e, x.lend()), None);
        verus_client::told_by_a_cursors_replace_with(&mut e, x.lend());
        verus_client::told_by_a_cursors_delete(&mut e, usize::MAX);
        verus_client::told_by_a_cursors_advance(&mut e, usize::MAX);
        verus_client::told_by_a_cursors_seek(&mut e, usize::MAX);
        verus_client::told_by_a_cursors_insert_run(&mut e, x.lend(), 0);
        assert_eq!(verus_client::told_by_a_cursors_places(&e), (n, n));
        verus_client::told_by_a_cursors_finish(e);
    }
    assert_eq!(seen(&col), was);
    {
        let mut e = verus_client::told_by_an_edit_at(&mut col, 1);
        assert_eq!(verus_client::told_by_a_cursors_places(&e), (1, 1));
        verus_client::told_by_a_cursors_delete(&mut e, usize::MAX);
        assert_eq!(verus_client::told_by_a_cursors_places(&e), (n, 1));
        verus_client::told_by_a_cursors_finish(e);
    }
    assert_eq!(seen(&col), was[..1]);
}

#[test]
fn edit_cursors() {
    let numbers: Vec<u64> = vec![5, 5, 6, 7, 7, 7, 9];
    let many: Vec<u64> = (0..1000).map(|i| i / 7 % 40).collect();
    cursor_clients::<u64, Leb128, LenIndex>(&numbers, &1, &6);
    cursor_clients::<u64, Leb128, LenIndex>(&many, &5, &0);
    cursor_clients::<u64, Leb128, SumIndex>(&numbers, &7, &0);
    cursor_clients::<u64, Leb128, SumIndex>(&many, &0, &3);
    cursor_clients::<Option<i64>, Leb128, LenIndex>(
        &[Some(-3), None, None, Some(i64::MAX), Some(-3)],
        &None,
        &Some(i64::MIN),
    );
    cursor_clients::<bool, Leb128, LenIndex>(&[true, true, false, true, false], &false, &true);
    cursor_clients::<bool, Leb128, LenIndex>(&[false; 4], &true, &true);
    let bytes = vec![b"ab".to_vec(), vec![], vec![], b"xyz".to_vec(), vec![0; 70]];
    cursor_clients::<Vec<u8>, Leb128, LenIndex>(&bytes, &b"ab".to_vec(), &vec![]);
    let words: Vec<String> = ["é", "", "x", "x", "long enough"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    cursor_clients::<String, Leb128, LenIndex>(&words, &"x".to_string(), &"ü".to_string());
    let marks: Vec<Mark> = (0..300).map(|i| Mark(i / 7 % 5 * 13)).collect();
    cursor_clients::<Mark, Leb128, AccIndex>(&marks, &Mark(13), &Mark(63));

    assert_eq!(verus_client::edit_as_documented(), [1, 9, 4, 7]);

    let lent: Vec<&[u8]> = bytes.iter().map(|b| &b[..]).collect();
    for at in 0..=bytes.len() {
        let mut col = VerifiedColumn::<Vec<u8>>::from_values(&lent);
        let taken = verus_client::bytes_taken_out(&mut col, at);
        assert_eq!(taken.as_ref(), bytes.get(at));
        let mut want = lent.clone();
        if at < want.len() {
            want.remove(at);
        }
        assert_eq!(col.to_vec(), want);
    }

    let flags = [true, true, false, false, true];
    for at in 0..flags.len() {
        let mut col = VerifiedColumn::<bool>::from_values(&flags);
        verus_client::flag_turned(&mut col, at);
        let mut want = flags;
        want[at] = !want[at];
        assert_eq!(col.to_vec(), want);
    }

    for values in [vec![], vec![0], many.clone(), vec![u64::MAX - 1, 0, 0]] {
        let mut col = VerifiedColumn::<u64>::from_values(&values);
        verus_client::each_one_more(&mut col);
        let want: Vec<u64> = values.iter().map(|v| v + 1).collect();
        assert_eq!(col.to_vec(), want);
    }

    for values in [
        vec![],
        vec![None],
        vec![None, None, Some(4), None, Some(4), Some(5), None],
        many.iter().map(|&v| (v % 3 != 0).then_some(v)).collect(),
        many.iter().map(|&v| Some(v)).collect(),
    ] {
        let mut col = VerifiedColumn::<Option<u64>>::from_values(&values);
        verus_client::absent_ones_dropped(&mut col);
        let want: Vec<Option<u64>> = values.iter().copied().filter(Option::is_some).collect();
        assert_eq!(col.to_vec(), want);
    }

    for (values, at, v) in [
        (vec![], 0, 9),
        (vec![3, 0, 4], 1, u64::MAX >> 8),
        (vec![3, 0, 4], 3, 0),
        (many.clone(), 500, 77),
    ] {
        let mut col = VerifiedPrefixColumn::<u64>::from_values(&values);
        verus_client::sums_after_a_cursor(&mut col, at, v);
        let mut want = values.clone();
        want.insert(at, v);
        assert_eq!(col.to_vec(), want);
        let again = VerifiedPrefixColumn::<u64>::from_values(&want);
        for i in [0, at, at + 1, want.len()] {
            assert_eq!(col.get_prefix(i), again.get_prefix(i));
        }
        assert_eq!(col.save(), again.save());
    }
}

#[cfg(feature = "bijou64")]
#[test]
fn edit_cursors_with_bijou64() {
    use hexane::Bijou64;
    let numbers: Vec<u64> = vec![5, 5, 6, 7, 7, 7, 9, 248, u64::MAX];
    cursor_clients::<u64, Bijou64, LenIndex>(&numbers, &1, &247);
    cursor_clients::<u64, Bijou64, SumIndex>(&numbers[..8], &7, &0);
    cursor_clients::<Option<i64>, Bijou64, LenIndex>(
        &[Some(-3), None, None, Some(i64::MAX), Some(-3)],
        &None,
        &Some(i64::MIN),
    );
    cursor_clients::<bool, Bijou64, LenIndex>(&[true, true, false, true, false], &false, &true);
    let bytes = vec![
        b"ab".to_vec(),
        vec![],
        vec![],
        b"xyz".to_vec(),
        vec![0; 300],
    ];
    cursor_clients::<Vec<u8>, Bijou64, LenIndex>(&bytes, &b"ab".to_vec(), &vec![]);
    let marks: Vec<Mark> = (0..300).map(|i| Mark(i / 7 % 5 * 13)).collect();
    cursor_clients::<Mark, Bijou64, AccIndex>(&marks, &Mark(13), &Mark(63));
}

// ── The edit cursor of a delta column ───────────────────────────────────────

/// The delta cursor's clients, on a column of `model`, which has six
/// values or more, with two values of the caller's.
fn delta_cursor_clients<T: VDeltaValue<C> + PartialEq + std::fmt::Debug, C: VCodec>(
    model: &[T],
    x: T,
    y: T,
) {
    let n = model.len();
    assert!(n >= 6);
    let fresh = || VerifiedDeltaColumn::<T, C>::from_values(model);
    let was = model.to_vec();

    for (at, del) in [(0, 0), (1, 2), (n, 0), (0, n), (n - 1, 1)] {
        let mut col = fresh();
        verus_client::delta_edit_like_a_splice(&mut col, at, del, x);
        let mut want = was.clone();
        want.splice(at..at + del, [x]);
        assert_eq!(col.to_vec(), want);
    }

    for at in [0, n - 1] {
        let mut col = fresh();
        let places = verus_client::delta_places_of_a_cursor(&mut col, at, y);
        assert_eq!(places, (at, at, at + 1, at + 2));
        let mut want = was.clone();
        want.splice(at..at + 1, [y, y]);
        assert_eq!(col.to_vec(), want);
    }

    for (a, b) in [(0, 0), (1, 3), (0, n), (n, n)] {
        let mut col = fresh();
        verus_client::delta_two_places_one_edit(&mut col, a, b, x, y);
        let mut want = was[..a].to_vec();
        want.push(x);
        want.extend_from_slice(&was[a..b]);
        want.push(y);
        want.extend_from_slice(&was[b..]);
        assert_eq!(col.to_vec(), want);
    }

    for (a, b, k) in [
        (0, 0, 0),
        (1, 2, 1),
        (2, usize::MAX, 5),
        (n, n + 3, usize::MAX),
        (3, 1, 0),
    ] {
        let mut col = fresh();
        verus_client::delta_only_passed_over(&mut col, a, b, k);
        assert_eq!(col.to_vec(), was);
    }

    for (a, b) in [(0, 0), (3, 1), (n, 0)] {
        let mut col = fresh();
        verus_client::delta_seek_back_stays(&mut col, a, b, x);
        let mut want = was.clone();
        want.insert(a, x);
        assert_eq!(col.to_vec(), want);
    }

    let mut col = fresh();
    verus_client::delta_pushed_by_a_cursor(&mut col, x);
    let mut want = was.clone();
    want.push(x);
    assert_eq!(col.to_vec(), want);

    for at in [0, 2, n, n + 5, usize::MAX] {
        let mut col = fresh();
        verus_client::delta_cut_short(&mut col, at);
        assert_eq!(col.to_vec(), was[..at.min(n)]);
    }

    for at in [0, 1, n] {
        let mut col = fresh();
        let there = verus_client::delta_what_is_there(&mut col, at);
        assert_eq!(there, was.get(at).copied());
        assert_eq!(col.to_vec(), was);
    }

    for at in [0, n - 1] {
        for v in [x, was[at]] {
            let mut col = fresh();
            verus_client::delta_put_in_its_place(&mut col, at, v);
            let mut want = was.clone();
            want[at] = v;
            assert_eq!(col.to_vec(), want);
        }
    }

    // each step on its own, one after another
    let mut col = fresh();
    {
        let mut e = verus_client::told_by_a_delta_edit(&mut col);
        assert_eq!(verus_client::told_by_a_delta_cursors_places(&e), (0, 0));
        verus_client::told_by_a_delta_cursors_advance(&mut e, 2);
        verus_client::told_by_a_delta_cursors_seek(&mut e, 1);
        assert_eq!(verus_client::told_by_a_delta_cursors_places(&e), (2, 2));
        verus_client::told_by_a_delta_cursors_insert(&mut e, x);
        verus_client::told_by_a_delta_cursors_insert_run(&mut e, y, 2);
        assert_eq!(
            verus_client::told_by_a_delta_cursors_peek(&mut e),
            Some(was[2])
        );
        verus_client::told_by_a_delta_cursors_replace_with(&mut e, y);
        verus_client::told_by_a_delta_cursors_delete(&mut e, 1);
        verus_client::told_by_a_delta_cursors_insert_runs(&mut e, &[]);
        verus_client::told_what_a_delta_step_keeps(&mut e, 0, x);
        assert_eq!(verus_client::told_by_a_delta_cursors_places(&e), (5, 8));
        verus_client::told_by_a_delta_cursors_finish(e);
    }
    let mut want = was[..2].to_vec();
    want.extend([x, y, y, y, x, x]);
    want.extend_from_slice(&was[5..]);
    assert_eq!(col.to_vec(), want);
    {
        let mut e = verus_client::told_by_a_delta_edit_at(&mut col, 1);
        assert_eq!(verus_client::told_by_a_delta_cursors_places(&e), (1, 1));
        verus_client::told_by_a_delta_cursors_delete(&mut e, usize::MAX);
        assert_eq!(
            verus_client::told_by_a_delta_cursors_places(&e),
            (want.len(), 1)
        );
        verus_client::told_by_a_delta_cursors_finish(e);
    }
    assert_eq!(col.to_vec(), want[..1]);
}

#[test]
fn delta_edit_cursors() {
    let numbers: Vec<u64> = vec![5, 5, 6, 7, 7, 7, 9];
    let many: Vec<u64> = (0..1000).map(|i| i / 7 % 40).collect();
    delta_cursor_clients::<u64, Leb128>(&numbers, 1, 6);
    delta_cursor_clients::<u64, Leb128>(&many, 5, 0);
    delta_cursor_clients::<i32, Leb128>(&[-3, 0, 0, 7, i32::MAX, i32::MIN], i32::MIN, 4);
    delta_cursor_clients::<Option<i64>, Leb128>(
        &[Some(-3), None, None, Some(1 << 40), Some(-3), None],
        None,
        Some(-(1 << 40)),
    );
    delta_cursor_clients::<Option<u32>, Leb128>(&[None; 6], Some(u32::MAX), None);

    assert_eq!(verus_client::delta_edit_as_documented(), [10, 25, 40, 45]);

    let mut col = VerifiedDeltaColumn::<u32>::from_values(&[1, 2]);
    verus_client::delta_runs_by_a_cursor(&mut col, 1);
    assert_eq!(col.to_vec(), [1, 12, 14, 16, 7, 9, 2]);

    let mut col = VerifiedDeltaColumn::<i64>::from_values(&[-5, 0, 0, 1 << 50, -(1 << 50)]);
    verus_client::delta_each_one_later(&mut col);
    assert_eq!(col.to_vec(), [-4, 1, 1, (1 << 50) + 1, -(1 << 50) + 1]);

    let mut col = VerifiedDeltaColumn::<Option<i32>>::from_values(&[
        None,
        Some(3),
        None,
        None,
        Some(-1),
        None,
    ]);
    verus_client::delta_nulls_dropped(&mut col);
    assert_eq!(col.to_vec(), [Some(3), Some(-1)]);

    // runs a caller made, put in where they pass the check: the run of
    // nulls only where the type has a null
    let runs = [
        DeltaRun {
            prefix: 10,
            delta: Some(-1),
            count: 2,
        },
        DeltaRun {
            prefix: 0,
            delta: None,
            count: 1,
        },
    ];
    let mut col = VerifiedDeltaColumn::<Option<u32>>::from_values(&[Some(1)]);
    assert!(verus_client::delta_checked_runs_by_a_cursor(
        &mut col, 0, &runs
    ));
    assert_eq!(col.to_vec(), [Some(9), Some(8), None, Some(1)]);
    let mut col = VerifiedDeltaColumn::<u32>::from_values(&[1]);
    assert!(!verus_client::delta_checked_runs_by_a_cursor(
        &mut col, 0, &runs
    ));
    assert_eq!(col.to_vec(), [1]);
}

#[cfg(feature = "bijou64")]
#[test]
fn delta_edit_cursors_with_bijou64() {
    use hexane::Bijou64;
    delta_cursor_clients::<u64, Bijou64>(&[5, 5, 6, 7, 7, 7, 9, 248], 1, 247);
    delta_cursor_clients::<Option<i64>, Bijou64>(
        &[Some(-3), None, None, Some(1 << 40), Some(-3), None],
        None,
        Some(7),
    );
}

// ── Ranges copied from one column into another ─────────────────────────────

#[test]
fn copies() {
    let dst: Vec<u64> = vec![1, 2, 3, 4, 5];
    let src: Vec<u64> = vec![7, 8, 9];

    let mut col = VerifiedColumn::<u64>::from_values(&dst);
    verus_client::copied_onto_the_end(&mut col, VerifiedColumn::<u64>::from_values(&src));
    assert_eq!(col.to_vec(), [1, 2, 3, 4, 5, 7, 8, 9]);

    for (at, del, start, end, want) in [
        (1, 2, 0, 2, vec![1, 7, 8, 4, 5]),
        (5, 0, 1, 100, vec![1, 2, 3, 4, 5, 8, 9]),
        (0, 0, 2, 1, vec![1, 2, 3, 4, 5]),
        (2, 3, 3, 3, vec![1, 2]),
    ] {
        let mut col = VerifiedColumn::<u64>::from_values(&dst);
        let from = VerifiedColumn::<u64>::from_values(&src);
        verus_client::copied_in_place(&mut col, from, at, del, start, end);
        assert_eq!(col.to_vec(), want);
    }

    let mut col = VerifiedColumn::<u64>::from_values(&dst);
    let from = VerifiedColumn::<u64>::from_values(&src);
    verus_client::copied_at_two_places(&mut col, from, 1, 3, 0, 1, 3);
    assert_eq!(col.to_vec(), [1, 7, 2, 3, 8, 9, 5]);

    let mut col = VerifiedColumn::<u64>::from_values(&dst);
    let from = VerifiedColumn::<u64>::from_values(&src);
    verus_client::copied_nothing(&mut col, from, 2);
    assert_eq!(col.to_vec(), dst);

    let mut col = VerifiedColumn::<u64>::from_values(&dst);
    let from = VerifiedColumn::<u64>::from_values(&src);
    let splices = [
        hexane::Splice {
            pos: 0,
            delete: 1,
            range: 2..3,
        },
        hexane::Splice {
            pos: 4,
            delete: 0,
            range: 3..9,
        },
    ];
    verus_client::told_by_copy_ranges(&mut col, from, &splices);
    assert_eq!(col.to_vec(), [9, 2, 3, 4, 5]);

    assert_eq!(verus_client::numbers_copied(), [1, 8, 9, 3, 10]);

    let mut col = VerifiedPrefixColumn::<u64>::from_values(&dst);
    let from = VerifiedPrefixColumn::<u64>::from_values(&src);
    verus_client::sums_after_a_copy(&mut col, from, 2);
    assert_eq!(col.to_vec(), [1, 2, 7, 8, 9, 3, 4, 5]);
    assert_eq!(col.get_total(7), 39);
}

// ── Ranges copied from one delta column into another ────────────────────────

#[test]
fn delta_copies() {
    let dst: Vec<i64> = vec![10, 20, 30, 40];
    let src: Vec<i64> = vec![-1, -2, -3];

    let mut col = VerifiedDeltaColumn::<i64>::from_values(&dst);
    verus_client::delta_copied_onto_the_end(
        &mut col,
        VerifiedDeltaColumn::<i64>::from_values(&src),
    );
    assert_eq!(col.to_vec(), [10, 20, 30, 40, -1, -2, -3]);

    for (at, del, start, end, want) in [
        (1, 2, 0, 2, vec![10, -1, -2, 40]),
        (4, 0, 1, 100, vec![10, 20, 30, 40, -2, -3]),
        (0, 0, 2, 1, vec![10, 20, 30, 40]),
        (2, 2, 3, 3, vec![10, 20]),
    ] {
        let mut col = VerifiedDeltaColumn::<i64>::from_values(&dst);
        let from = VerifiedDeltaColumn::<i64>::from_values(&src);
        verus_client::delta_copied_in_place(&mut col, from, at, del, start, end);
        assert_eq!(col.to_vec(), want);
    }

    let mut col = VerifiedDeltaColumn::<Option<u32>>::from_values(&[Some(1), None, Some(3)]);
    let from = VerifiedDeltaColumn::<Option<u32>>::from_values(&[None, Some(9)]);
    let splices = [hexane::Splice {
        pos: 1,
        delete: 1,
        range: 0..2,
    }];
    verus_client::told_by_delta_copy_ranges(&mut col, from, &splices);
    assert_eq!(col.to_vec(), [Some(1), None, Some(9), Some(3)]);

    assert_eq!(verus_client::timestamps_copied(), [100, 6, 7, 300]);
}

// ── Repositioning an iterator: VShiftable, Unshift, Ranges ──────────────────

#[test]
fn repositioned() {
    let numbers: Vec<u64> = (1..=8).collect();
    let col = VerifiedColumn::<u64>::from_values(&numbers);
    let ranges = [1..3, 5..7, 7..7, 9..12];
    assert_eq!(verus_client::kept_over_ranges(&col, &ranges), [2, 3, 6, 7]);
    assert_eq!(verus_client::numbers_of_ranges(), [2, 3, 6, 7]);
    assert_eq!(verus_client::peeked_then_given(&col), (Some(1), Some(1)));
    let empty = VerifiedColumn::<u64>::new();
    assert_eq!(verus_client::peeked_then_given(&empty), (None, None));
    assert_eq!(verus_client::unshifted_to(&col, 0, 2), Some(1));
    assert_eq!(verus_client::unshifted_to(&col, 4, 9), Some(5));
    assert_eq!(verus_client::unshifted_to(&col, 4, 4), None);
    assert_eq!(verus_client::unshifted_to(&col, 8, 9), None);

    let mut it = col.iter();
    assert_eq!(verus_client::sought_then_advanced(&mut it, 2, 3), Some(3));
    assert_eq!(it.next(), Some(7));
    let mut it = col.iter();
    assert_eq!(verus_client::told_by_get_pos_and_max(&it), (0, 8));
    assert_eq!(verus_client::told_by_get_range(&it), 0..8);
    verus_client::told_by_set_max(&mut it, 6);
    verus_client::told_by_advance_to(&mut it, 2);
    assert_eq!(verus_client::told_by_shift_step(&mut it), Some(3));
    let copy = verus_client::told_by_shift_dup(&it);
    assert_eq!(copy.collect::<Vec<_>>(), [4, 5, 6]);
    verus_client::told_by_shift(&mut it, 1, 5);
    assert_eq!(it.collect::<Vec<_>>(), [4, 5]);
    let mut it = col.iter();
    assert_eq!(verus_client::told_by_shift_nth(&mut it, 2), Some(3));
    assert_eq!(it.next(), Some(4));
    let mut it = col.iter();
    assert_eq!(verus_client::told_by_shift_next(&mut it, 6, 20), Some(7));
    assert_eq!(it.next(), Some(8));

    let mut u = verus_client::told_by_unshift(col.iter());
    assert_eq!(verus_client::told_by_unshift_peek_next(&mut u), Some(1));
    verus_client::told_by_unshift_shift(&mut u, 3, 5);
    assert_eq!(u.collect::<Vec<_>>(), [4, 5]);
    let mut r = verus_client::told_by_ranges(col.iter(), &ranges);
    assert_eq!(verus_client::told_by_ranges_next(&mut r), Some(2));
    assert_eq!(r.collect::<Vec<_>>(), [3, 6, 7]);
}

// ── Repositioning the prefix, accumulator and delta iterators ──────────────

#[test]
fn repositioned_others() {
    let col = VerifiedDeltaColumn::<i64>::from_values(&[10, 20, 30, 40, 50]);
    assert_eq!(
        verus_client::delta_kept_over_ranges(&col, &[1..3, 4..9]),
        [20, 30, 50]
    );
    assert_eq!(verus_client::delta_first_by_lookahead(&col, 3, 4), Some(40));
    assert_eq!(verus_client::delta_first_by_lookahead(&col, 3, 3), None);
    assert_eq!(
        verus_client::delta_two_of_range(&col, 3, 9),
        (Some(40), Some(50))
    );
    assert_eq!(
        verus_client::delta_two_of_range(&col, 4, 9),
        (Some(50), None)
    );
    let (mut u, r) = verus_client::delta_told_by_adapters(col.iter(), col.iter(), &[1..3]);
    assert_eq!(u.next(), Some(10));
    assert_eq!(r.collect::<Vec<_>>(), [20, 30]);

    let p = VerifiedPrefixColumn::<u64>::from_values(&[1, 2, 3, 4]);
    for f in [
        verus_client::prefix_first_by_lookahead,
        verus_client::prefix_first_over_ranges,
    ] {
        let pv = f(&p, 2, 4).unwrap();
        assert_eq!((pv.value, pv.total()), (3, 6));
        assert!(f(&p, 4, 9).is_none());
    }
    let (x, y) = verus_client::prefix_two_of_range(&p, 1, 3);
    assert_eq!((x.unwrap().total(), y.unwrap().total()), (3, 6));
    let (x, y) = verus_client::prefix_two_of_range(&p, 3, 9);
    assert_eq!((x.unwrap().total(), y), (10, None));
    let (mut u, r) =
        verus_client::prefix_told_by_adapters(p.prefix_iter(), p.prefix_iter(), &[1..2]);
    assert_eq!(u.next().unwrap().total(), 1);
    assert_eq!(r.map(|pv| pv.total()).collect::<Vec<_>>(), [3]);

    let marks: Vec<Mark> = (0..20).map(|i| Mark(i % 3 * 13)).collect();
    let acc = VerifiedAccColumn::<Mark>::from_values(&marks);
    for f in [
        verus_client::acc_first_by_lookahead,
        verus_client::acc_first_over_ranges,
    ] {
        let av = f(&acc, 5, 9).unwrap();
        assert_eq!(
            (av.value.0, av.total.0),
            (marks[5].0, flags_of(&marks[..6]))
        );
        assert!(f(&acc, 9, 9).is_none());
    }
    let (x, y) = verus_client::acc_two_of_range(&acc, 18, 25);
    assert_eq!(
        (x.unwrap().total.0, y.unwrap().total.0),
        (flags_of(&marks[..19]), flags_of(&marks[..20]))
    );
    let (mut u, r) =
        verus_client::acc_told_by_adapters(acc.prefix_iter(), acc.prefix_iter(), &[1..2]);
    assert_eq!(u.next().unwrap().total.0, flags_of(&marks[..1]));
    assert_eq!(
        r.map(|av| av.total.0).collect::<Vec<_>>(),
        [flags_of(&marks[..2])]
    );
}

// ── Repositioning the runs of a column ─────────────────────────────────────

#[test]
fn repositioned_runs() {
    let col = VerifiedColumn::<u64>::from_values(&[7, 7, 7, 8, 8, 9]);
    let r = verus_client::run_at_range(&col, 1, 5).unwrap();
    assert_eq!((r.count, r.value), (2, 7));
    assert!(verus_client::run_at_range(&col, 6, 9).is_none());
    let r = verus_client::run_sought(&col, 3).unwrap();
    assert_eq!((r.count, r.value), (2, 8));
    assert!(verus_client::run_sought(&col, 6).is_none());
    let r = verus_client::run_after(&col, 4).unwrap();
    assert_eq!((r.count, r.value), (1, 8));
    let r = verus_client::run_over_range(&col, 2, 4).unwrap();
    assert_eq!((r.count, r.value), (1, 7));
    let r = verus_client::run_peeked(&col).unwrap();
    assert_eq!((r.count, r.value), (3, 7));
    let (mut u, w) =
        verus_client::runs_told_by_adapters(col.iter().runs(), col.iter().runs(), &[2..4]);
    assert_eq!(u.next().map(|r| (r.count, r.value)), Some((3, 7)));
    assert_eq!(
        w.map(|r| (r.count, r.value)).collect::<Vec<_>>(),
        [(1, 7), (1, 8)]
    );
}

// ── Repositioning the runs of a delta column ───────────────────────────────

#[test]
fn repositioned_delta_runs() {
    let col = VerifiedDeltaColumn::<i64>::from_values(&[10, 20, 30, 40, 41, 42, 50]);
    let r = verus_client::delta_run_at_range(&col, 1, 9).unwrap();
    assert_eq!((r.prefix, r.delta, r.count), (10, Some(10), 3));
    assert!(verus_client::delta_run_at_range(&col, 7, 9).is_none());
    let r = verus_client::delta_run_sought(&col, 4).unwrap();
    assert_eq!((r.prefix, r.delta, r.count), (40, Some(1), 2));
    let r = verus_client::delta_run_after(&col, 2).unwrap();
    assert_eq!((r.prefix, r.delta, r.count), (20, Some(10), 2));
    let r = verus_client::delta_run_over_range(&col, 4, 5).unwrap();
    assert_eq!((r.prefix, r.delta, r.count), (40, Some(1), 1));
    let r = verus_client::delta_run_peeked(&col).unwrap();
    assert_eq!((r.prefix, r.delta, r.count), (0, Some(10), 4));
    let (mut u, w) =
        verus_client::delta_runs_told_by_adapters(col.iter().runs(), col.iter().runs(), &[5..7]);
    assert_eq!(u.next().map(|r| r.count), Some(4));
    assert_eq!(
        w.map(|r| (r.prefix, r.delta, r.count)).collect::<Vec<_>>(),
        [(41, Some(1), 1), (42, Some(8), 1)]
    );
}

// ── Loading a column run by run ────────────────────────────────────────────

#[test]
fn loaded_by_runs() {
    let col = VerifiedColumn::<u64>::from_values(&[3, 3, 4, 5, 5, 5]);
    let bytes = col.save();
    let (run, c) = verus_client::first_run_and_column::<u64, Leb128>(&bytes).unwrap();
    assert_eq!(run.map(|r| (r.count, r.value)), Some((2, 3)));
    assert_eq!(c.to_vec(), [3, 3, 4, 5, 5, 5]);
    let (runs, c) = verus_client::loaded_runs::<u64, Leb128, LenIndex>(&bytes).unwrap();
    let runs: Vec<(usize, u64)> = runs.iter().map(|r| (r.count, r.value)).collect();
    assert_eq!(runs, [(2, 3), (1, 4), (3, 5)]);
    assert_eq!(c.to_vec(), [3, 3, 4, 5, 5, 5]);
    let (run, c) = verus_client::filled_by_load::<u64, Leb128>(9, 4);
    assert_eq!(run.map(|r| (r.count, r.value)), Some((4, 9)));
    assert_eq!(c.unwrap().to_vec(), [9; 4]);
}
