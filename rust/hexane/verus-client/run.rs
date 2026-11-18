//! Runs the clients Verus checked: each call meets its client's
//! `requires`, and the client's own run-time work (loading, saving,
//! unwrapping) must not panic.
use hexane::verified::{
    LenIndex, SumIndex, VerifiedColumn, VerifiedDecoder, VerifiedEncoder, VerifiedEncoderState,
    VerifiedPrefixColumn,
};
use hexane::{Leb128, Run};
use verus_client::Action;

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
