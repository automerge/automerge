use hexane::v1::Column;
use rand::{rng, Rng, RngExt};
use std::time::Instant;

fn rand_string(len: usize) -> String {
    let mut r = rng();
    (0..len)
        .map(|_| (b'a' + (r.random::<u8>() % 26)) as char)
        .collect()
}
fn rand_strings(n: usize, len: usize) -> Vec<String> {
    (0..n).map(|_| rand_string(len)).collect()
}

fn bench(name: &str, n: u64, mut f: impl FnMut()) {
    for _ in 0..100 {
        f();
    }
    let start = Instant::now();
    for _ in 0..n {
        f();
    }
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    println!("{name}: {ns:.0} ns/iter");
}

fn main() {
    let slen = 8;

    // First: measure just the BIT rebuild cost at different slab counts.
    println!("=== BIT rebuild cost ===");
    for &ms in &[8, 16, 32, 64] {
        let col = Column::<String>::from_values_with_max_segments(
            (0..100_000).map(|_| rand_string(slen)).collect(),
            ms,
        );
        let slabs = col.slab_count();
        // We can't directly call rebuild_bit from here, but we can measure
        // from_values (which does one rebuild) vs the slab count.
        let start = Instant::now();
        for _ in 0..100 {
            let _ = Column::<String>::from_values_with_max_segments(
                (0..100_000).map(|_| rand_string(slen)).collect(),
                ms,
            );
        }
        let build_ns = start.elapsed().as_nanos() / 100;
        println!("  ms={ms:3} slabs={slabs:6} from_values: {build_ns:>10} ns");
    }

    for &ms in &[8, 16, 32, 64] {
        println!("\n=== max_segments={ms}, string_len={slen} ===");

        let col_100k = Column::<String>::from_values_with_max_segments(
            (0..100_000).map(|_| rand_string(slen)).collect(),
            ms,
        );
        println!("  slabs={}", col_100k.slab_count());

        // replace_1
        let mut c = col_100k.clone();
        bench(&format!("  ms={ms} replace_1 "), 10_000, || {
            let pos = rng().next_u64() as usize % c.len();
            c.splice(pos, 1, [rand_string(slen)]);
        });

        // replace_10
        let mut c = col_100k.clone();
        bench(&format!("  ms={ms} replace_10"), 1_000, || {
            let len = c.len();
            if len <= 10 {
                return;
            }
            let pos = rng().next_u64() as usize % (len - 10);
            c.splice(pos, 10, rand_strings(10, slen));
        });

        // Show slab count after 1000 replace_10 ops
        println!("  slabs after 1000 replace_10: {}", c.slab_count());

        // insert_1
        let mut c = col_100k.clone();
        bench(&format!("  ms={ms} insert_1 "), 10_000, || {
            let pos = rng().next_u64() as usize % c.len();
            c.splice(pos, 0, [rand_string(slen)]);
        });

        // delete_1
        let mut c = col_100k.clone();
        bench(&format!("  ms={ms} delete_1 "), 10_000, || {
            if c.len() > 1 {
                let pos = rng().next_u64() as usize % c.len();
                c.splice(pos, 1, std::iter::empty::<String>());
            }
        });
    }
}
