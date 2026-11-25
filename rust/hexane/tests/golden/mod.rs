//! What the old API did, kept after it is gone.
//!
//! The tests of the verified API once ran the old API's columns beside
//! the verified ones and checked that both did the same: wrote the same
//! bytes, loaded the same values, refused the same bytes. While the old
//! API existed, each such check compared what the verified API did with
//! what the old API did, and a run of the tests built with
//! `HEXANE_GOLDEN_RECORD` set printed, for each test, a digest of
//! everything the verified side gave to those checks. Those digests are
//! `verified_api.txt`, and they now stand in for the old API: each check
//! is now a [`note`] of what the verified side gives, which adds it to the
//! test's digest, and at the end of the test the digest is checked against
//! the one recorded. So each test still checks that the verified API does
//! what the old API did, in every case the test makes.
//!
//! A digest is kept per test and per pointer width (the tests take other
//! paths on 32-bit targets). A test with no recorded digest, written
//! after the old API was gone, is not checked against one.

use std::cell::RefCell;
use std::fmt::Debug;

const RECORDED: &str = include_str!("verified_api.txt");

/// Whether this build records the digests rather than checks them.
const RECORD: bool = option_env!("HEXANE_GOLDEN_RECORD").is_some();

struct Digest {
    name: &'static str,
    count: u64,
    hash: u64,
}

thread_local! {
    static CURRENT: RefCell<Option<Digest>> = const { RefCell::new(None) };
}

/// What a test has given to [`note`], checked against what was recorded
/// when it ends (unless it ends by panicking).
pub struct Test(());

/// Starts the digest of the test `name` (its module path and name).
pub fn test(name: &'static str) -> Test {
    CURRENT.with(|c| {
        *c.borrow_mut() = Some(Digest {
            name,
            count: 0,
            hash: 0xcbf2_9ce4_8422_2325,
        })
    });
    Test(())
}

/// Adds what the verified API gave, as `{:?}` prints it, to the test's
/// digest. The word `Verified` is left out, so that a renamed type does not
/// change a digest.
///
/// The printed text is hashed as it is printed, a piece at a time, and
/// never held whole in memory (some values noted print as megabytes):
/// once to count its bytes, which come first in the digest, and once to
/// hash them.
pub fn note<T: Debug + ?Sized>(x: &T) {
    CURRENT.with(|c| {
        if let Some(d) = c.borrow_mut().as_mut() {
            let mut len = 0u64;
            stripped(x, &mut |b| len += b.len() as u64);
            let mut hash = d.hash;
            let mut add = |b: &[u8]| {
                for &byte in b {
                    hash ^= u64::from(byte);
                    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
                }
            };
            add(&len.to_le_bytes());
            stripped(x, &mut add);
            d.count += 1;
            d.hash = hash;
        }
    });
}

const WORD: &[u8] = b"Verified";

/// Gives `{:?}` of `x` to `f`, piece by piece, without the word `Verified`:
/// what `format!("{x:?}").replace("Verified", "")` holds. (No proper start
/// of the word is also an end of it, so a byte that breaks a match can only
/// start a new one.)
fn stripped<T: Debug + ?Sized>(x: &T, f: &mut dyn FnMut(&[u8])) {
    struct Strip<'f> {
        matched: usize,
        f: &'f mut dyn FnMut(&[u8]),
    }
    impl std::fmt::Write for Strip<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            for &b in s.as_bytes() {
                if b == WORD[self.matched] {
                    self.matched += 1;
                    if self.matched == WORD.len() {
                        self.matched = 0;
                    }
                    continue;
                }
                if self.matched > 0 {
                    (self.f)(&WORD[..self.matched]);
                    self.matched = 0;
                    if b == WORD[0] {
                        self.matched = 1;
                        continue;
                    }
                }
                (self.f)(&[b]);
            }
            Ok(())
        }
    }
    let mut w = Strip { matched: 0, f };
    std::fmt::write(&mut w, format_args!("{x:?}")).unwrap();
    (w.f)(&WORD[..w.matched]);
}

fn width() -> u32 {
    usize::BITS
}

impl Drop for Test {
    fn drop(&mut self) {
        let Some(d) = CURRENT.with(|c| c.borrow_mut().take()) else {
            return;
        };
        if std::thread::panicking() {
            return;
        }
        let line = format!("{} {} {} {:016x}", width(), d.name, d.count, d.hash);
        if RECORD {
            println!("golden {line}");
            return;
        }
        let prefix = format!("{} {} ", width(), d.name);
        if let Some(recorded) = RECORDED.lines().find(|l| l.starts_with(&prefix)) {
            assert_eq!(
                line, recorded,
                "what the verified API did is not what the old API did"
            );
        }
    }
}
