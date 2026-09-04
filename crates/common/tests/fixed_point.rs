//! Exactness tests for the fixed-point layer.
//!
//! These are the tests that protect data which cannot be re-captured. If one of
//! them fails, do not adjust the test.

use kalshi_common::{
    GridSnap, Notional, ParseFixedError, PriceRange, PriceRanges, Px, Qty, Side, Ticker, PX_SCALE,
    QTY_SCALE,
};

// ---------------------------------------------------------------------------
// The required parser matrix
// ---------------------------------------------------------------------------

#[test]
fn parses_the_documented_wire_values_exactly() {
    // (input, expected micro-dollars)
    let cases = [
        ("0", 0),
        ("0.0010", 1_000),
        ("0.5500", 550_000),
        ("0.9999", 999_900),
        ("1.0000", 1_000_000),
        // The smallest documented tick is $0.0001, not $0.001.
        ("0.0001", 100),
        ("0.0000", 0),
        // Six decimals is the internal scale; all of these are exact.
        ("0.123456", 123_456),
        ("12.500000", 12_500_000),
        // THREE decimals. The spec's own examples mix widths within a single
        // channel -- orderbook_snapshot shows "0.0800" while orderbook_delta
        // shows "0.960" for the same market. A parser that assumed a fixed
        // 4-decimal width would read "0.960" as 0.0960 and be wrong by 10x on
        // every delta, silently.
        ("0.960", 960_000),
        ("0.001", 1_000),
        ("0.999", 999_000),
        ("1.000", 1_000_000),
        // One and two decimals, for the same reason.
        ("0.5", 500_000),
        ("0.55", 550_000),
        ("1.0", 1_000_000),
        // Five decimals, between the wire width and our internal scale.
        ("0.00001", 10),
    ];
    for (input, expected) in cases {
        assert_eq!(
            Px::parse_dollars(input).map(Px::micros),
            Ok(expected),
            "parsing {input:?}"
        );
    }
}

#[test]
fn decimal_width_does_not_change_the_value() {
    // The same price written at every width the exchange uses must parse to
    // the same integer. This is the property that makes mixed-width wire data
    // safe.
    let widths = ["0.96", "0.960", "0.9600", "0.96000", "0.960000"];
    let expected = Px::parse_dollars("0.960").map(Px::micros);
    assert_eq!(expected, Ok(960_000));
    for raw in widths {
        assert_eq!(
            Px::parse_dollars(raw).map(Px::micros),
            expected,
            "width-dependent parse for {raw:?}"
        );
    }
    // Same for quantities: _fp is documented as 2 decimals but accepts 0-2.
    for raw in ["10", "10.0", "10.00"] {
        assert_eq!(
            Qty::parse_fp(raw).map(Qty::fp_units),
            Ok(1_000),
            "for {raw:?}"
        );
    }
}

#[test]
fn parses_negatives() {
    assert_eq!(Px::parse_dollars("-0.5500").map(Px::micros), Ok(-550_000));
    assert_eq!(Px::parse_dollars("-1.0000").map(Px::micros), Ok(-1_000_000));
    assert_eq!(Px::parse_dollars("-0").map(Px::micros), Ok(0));
    assert_eq!(Px::parse_dollars("-0.000000").map(Px::micros), Ok(0));
    // delta_fp arrives signed on the wire.
    assert_eq!(Qty::parse_fp("-54.00").map(Qty::fp_units), Ok(-5_400));
}

#[test]
fn rejects_precision_it_cannot_represent_instead_of_truncating() {
    // Seven significant decimals at scale 1e-6: the 7th digit is non-zero, so
    // accepting this would silently discard information.
    let err = Px::parse_dollars("0.1234567").unwrap_err();
    assert!(
        matches!(&err, ParseFixedError::ExcessPrecision { excess, scale, .. }
                 if excess == "7" && *scale == 6),
        "got {err:?}"
    );

    // Qty is scale 1e-2: a half-hundredth of a contract is not representable.
    let err = Qty::parse_fp("1.005").unwrap_err();
    assert!(
        matches!(err, ParseFixedError::ExcessPrecision { .. }),
        "got {err:?}"
    );

    // The error is loud enough to act on: it names the input and the digits.
    assert!(err.to_string().contains("Refusing to truncate"));
}

#[test]
fn accepts_excess_trailing_zeros_because_nothing_is_lost() {
    // More digits than the scale, but every excess digit is zero. This is
    // exactly representable, so rejecting it would drop a capturable message
    // for no benefit. Contrast with the test above.
    assert_eq!(
        Px::parse_dollars("1.0000000").map(Px::micros),
        Ok(1_000_000)
    );
    assert_eq!(
        Px::parse_dollars("0.55000000000").map(Px::micros),
        Ok(550_000)
    );
    assert_eq!(Qty::parse_fp("10.0000").map(Qty::fp_units), Ok(1_000));
}

#[test]
fn rejects_malformed_input() {
    use ParseFixedError as E;
    type Check = fn(&E) -> bool;
    let cases: &[(&str, Check)] = &[
        ("", |e| matches!(e, E::Empty)),
        ("-", |e| matches!(e, E::MissingIntegerDigits { .. })),
        (".5", |e| matches!(e, E::MissingIntegerDigits { .. })),
        ("5.", |e| matches!(e, E::MissingFractionDigits { .. })),
        ("1.2.3", |e| matches!(e, E::MultipleDecimalPoints { .. })),
        ("abc", |e| matches!(e, E::InvalidChar { .. })),
        ("0x10", |e| matches!(e, E::InvalidChar { .. })),
        ("1e6", |e| matches!(e, E::InvalidChar { .. })),
        ("+1.0", |e| matches!(e, E::InvalidChar { .. })),
        (" 1.0", |e| matches!(e, E::InvalidChar { .. })),
        ("1.0 ", |e| matches!(e, E::InvalidChar { .. })),
        ("1,0", |e| matches!(e, E::InvalidChar { .. })),
        ("NaN", |e| matches!(e, E::InvalidChar { .. })),
        ("inf", |e| matches!(e, E::InvalidChar { .. })),
        ("１.０", |e| matches!(e, E::InvalidChar { .. })), // full-width digits
        ("--1", |e| matches!(e, E::InvalidChar { .. })),
    ];
    for (input, expected) in cases {
        let err = Px::parse_dollars(input).expect_err(&format!("{input:?} should not parse"));
        assert!(expected(&err), "{input:?} produced unexpected {err:?}");
    }
}

#[test]
fn rejects_values_that_overflow_i64() {
    let huge = "9".repeat(30);
    assert!(matches!(
        Px::parse_dollars(&huge),
        Err(ParseFixedError::Overflow { .. })
    ));
}

#[test]
fn parsing_never_routes_through_f64() {
    // 0.1 + 0.2 != 0.3 in binary floating point. If any f64 were involved in
    // the parse path, accumulating these in micro-dollars would not land on an
    // exact value. This is a canary, not a proof; the real guarantee is the
    // crate-level `deny(clippy::float_arithmetic)`.
    let a = Px::parse_dollars("0.100000").map(Px::micros);
    let b = Px::parse_dollars("0.200000").map(Px::micros);
    let c = Px::parse_dollars("0.300000").map(Px::micros);
    assert_eq!(a.and_then(|a| b.map(|b| a + b)), c);
}

// ---------------------------------------------------------------------------
// Round-tripping: the property the Parquet dual-column scheme depends on
// ---------------------------------------------------------------------------

#[test]
fn every_price_round_trips_through_its_string_form() {
    // Storage writes `price_raw` beside `price_micros`. Re-parsing the raw
    // column must reproduce the integer column exactly, for every value.
    let mut value = -2_000_000i64;
    while value <= 2_000_000 {
        let px = Px::from_micros(value);
        let rendered = px.to_dollar_string();
        assert_eq!(
            Px::parse_dollars(&rendered).map(Px::micros),
            Ok(value),
            "round trip failed for {value} rendered as {rendered:?}"
        );
        value += 9_973; // a prime stride, to avoid only testing round numbers
    }
}

#[test]
fn every_quantity_round_trips_through_its_string_form() {
    for units in [-1_000_000i64, -5_400, -1, 0, 1, 99, 100, 1_000, 123_456_789] {
        let qty = Qty::from_fp_units(units);
        assert_eq!(
            Qty::parse_fp(&qty.to_fp_string()).map(Qty::fp_units),
            Ok(units)
        );
    }
}

#[test]
fn wire_strings_round_trip_back_to_their_original_text_shape() {
    // Not byte-identical -- we always emit full scale -- but numerically equal.
    for raw in ["0.5500", "0.0001", "0.9999", "1.0000", "0"] {
        let parsed = Px::parse_dollars(raw).expect("valid");
        let reparsed = Px::parse_dollars(&parsed.to_dollar_string()).expect("valid");
        assert_eq!(parsed, reparsed, "for {raw:?}");
    }
}

// ---------------------------------------------------------------------------
// Reciprocal pricing
// ---------------------------------------------------------------------------

#[test]
fn yes_and_no_sum_to_exactly_one_dollar() {
    // The identity that makes the book's canonical-YES storage safe. In f64,
    // 0.43 + 0.57 != 1.0; here it is exact for every price on the grid.
    let mut micros = 0i64;
    while micros <= PX_SCALE {
        let yes = Px::from_micros(micros);
        let no = yes.complement();
        assert_eq!(
            yes.checked_add(no),
            Some(Px::ONE_DOLLAR),
            "YES {yes} + NO {no} != $1.00"
        );
        assert_eq!(
            no.complement(),
            yes,
            "complement is not an involution at {yes}"
        );
        micros += 1;
        if micros > 1_000 {
            micros += 997; // sample the rest of the range sparsely
        }
    }
}

#[test]
fn the_documented_example_holds() {
    // "A YES bid at $0.4300 is economically a NO ask at $0.5700."
    let yes_bid = Px::parse_dollars("0.4300").expect("valid");
    assert_eq!(
        yes_bid.complement(),
        Px::parse_dollars("0.5700").expect("valid")
    );
}

#[test]
fn spread_and_mid_are_exact() {
    // Best YES bid $0.42, best NO bid $0.56 => implied YES ask $0.44,
    // spread $0.02, mid $0.43. All exact in fixed point.
    let yes_bid = Px::parse_dollars("0.4200").expect("valid");
    let no_bid = Px::parse_dollars("0.5600").expect("valid");
    let yes_ask = no_bid.complement();
    assert_eq!(yes_ask, Px::parse_dollars("0.4400").expect("valid"));
    assert_eq!(
        yes_ask.checked_sub(yes_bid),
        Some(Px::parse_dollars("0.0200").expect("valid"))
    );
    assert_eq!(
        yes_bid.midpoint(yes_ask),
        Px::parse_dollars("0.4300").expect("valid")
    );
}

#[test]
fn midpoint_rounds_toward_negative_infinity_consistently() {
    let a = Px::from_micros(1);
    let b = Px::from_micros(2);
    assert_eq!(a.midpoint(b), Px::from_micros(1));
    let a = Px::from_micros(-1);
    let b = Px::from_micros(-2);
    assert_eq!(a.midpoint(b), Px::from_micros(-2));
}

// ---------------------------------------------------------------------------
// Notional
// ---------------------------------------------------------------------------

#[test]
fn notional_is_the_exact_product_at_scale_1e8() {
    // $0.55 x 10 contracts = $5.50
    let px = Px::parse_dollars("0.5500").expect("valid");
    let qty = Qty::parse_fp("10.00").expect("valid");
    let n = Notional::from_px_qty(px, qty).expect("no overflow");
    assert_eq!(n.units(), 550_000 * 1_000);
    assert_eq!(n.to_dollar_string(), "5.50000000");
    assert_eq!(n.to_micro_dollars(), 5_500_000);
}

#[test]
fn notional_handles_fractional_contracts() {
    // 0.01 contracts -- the documented minimum granularity -- at $0.9999.
    let px = Px::parse_dollars("0.9999").expect("valid");
    let qty = Qty::parse_fp("0.01").expect("valid");
    let n = Notional::from_px_qty(px, qty).expect("no overflow");
    assert_eq!(n.units(), 999_900);
    assert_eq!(n.to_dollar_string(), "0.00999900");
}

#[test]
fn notional_scale_is_the_sum_of_its_factors_scales() {
    // The reason no rounding is needed to *form* a notional.
    assert_eq!(
        kalshi_common::NOTIONAL_SCALE_DIGITS,
        kalshi_common::PX_SCALE_DIGITS + kalshi_common::QTY_SCALE_DIGITS
    );
    let one = Notional::from_px_qty(Px::ONE_DOLLAR, Qty::ONE_CONTRACT).expect("no overflow");
    assert_eq!(one.units(), kalshi_common::NOTIONAL_SCALE);
    assert_eq!(PX_SCALE * QTY_SCALE, kalshi_common::NOTIONAL_SCALE);
}

#[test]
fn notional_rounding_is_half_away_from_zero_and_symmetric() {
    // Exactly half a micro-dollar at scale 1e-8 is 50 units.
    assert_eq!(Notional::from_units(50).to_micro_dollars(), 1);
    assert_eq!(Notional::from_units(-50).to_micro_dollars(), -1);
    assert_eq!(Notional::from_units(49).to_micro_dollars(), 0);
    assert_eq!(Notional::from_units(-49).to_micro_dollars(), 0);
    assert_eq!(Notional::from_units(150).to_micro_dollars(), 2);
    assert_eq!(Notional::from_units(-150).to_micro_dollars(), -2);

    // Symmetry is the point: summing a mixed-sign series must not drift.
    for units in [-150i64, -50, -49, -1, 0, 1, 49, 50, 150, 12_345] {
        assert_eq!(
            Notional::from_units(units).to_micro_dollars(),
            -Notional::from_units(-units).to_micro_dollars(),
            "asymmetric rounding at {units}"
        );
    }
}

#[test]
fn notional_reports_overflow_rather_than_wrapping() {
    let huge_px = Px::from_micros(i64::MAX);
    let huge_qty = Qty::from_fp_units(i64::MAX);
    assert_eq!(Notional::from_px_qty(huge_px, huge_qty), None);
}

// ---------------------------------------------------------------------------
// Tick grid
// ---------------------------------------------------------------------------

fn band(start: &str, end: &str, step: &str) -> PriceRange {
    PriceRange {
        start: Px::parse_dollars(start).expect("valid"),
        end: Px::parse_dollars(end).expect("valid"),
        step: Px::parse_dollars(step).expect("valid"),
    }
}

fn nonuniform_grid() -> PriceRanges {
    // Fine ticks in the tails, penny ticks in the middle -- the shape that
    // makes hardcoding a single tick size wrong.
    PriceRanges::new(vec![
        band("0.0001", "0.0500", "0.0001"),
        band("0.0500", "0.9500", "0.0100"),
        band("0.9500", "0.9999", "0.0001"),
    ])
    .expect("valid grid")
}

#[test]
fn validates_prices_against_the_published_grid() {
    let grid = nonuniform_grid();
    assert!(grid.is_valid(Px::parse_dollars("0.0001").expect("valid")));
    assert!(grid.is_valid(Px::parse_dollars("0.0234").expect("valid")));
    assert!(grid.is_valid(Px::parse_dollars("0.4300").expect("valid")));
    assert!(grid.is_valid(Px::parse_dollars("0.9876").expect("valid")));

    // On a $0.01 grid in the middle band, sub-penny prices are off-grid.
    assert!(!grid.is_valid(Px::parse_dollars("0.4301").expect("valid")));
    // Outside every band.
    assert!(!grid.is_valid(Px::parse_dollars("1.5000").expect("valid")));
}

#[test]
fn snaps_to_the_nearest_valid_price() {
    let grid = nonuniform_grid();
    let px = |s: &str| Px::parse_dollars(s).expect("valid");

    // Already on grid: reported as Exact, and untouched.
    assert_eq!(
        grid.snap_to_grid(px("0.4300")),
        Some(GridSnap::Exact(px("0.4300")))
    );

    // Inside a band but between ticks: Snapped, not Clamped.
    assert_eq!(
        grid.snap_to_grid(px("0.4301")),
        Some(GridSnap::Snapped(px("0.4300")))
    );
    assert_eq!(
        grid.snap_to_grid(px("0.4299")),
        Some(GridSnap::Snapped(px("0.4300")))
    );
    // Ties resolve upward.
    assert_eq!(
        grid.snap_to_grid(px("0.4350")),
        Some(GridSnap::Snapped(px("0.4400")))
    );

    // Everything snapped is, by definition, valid.
    for raw in ["0.0037", "0.4301", "0.9501"] {
        let snap = grid.snap_to_grid(px(raw)).expect("non-empty grid");
        assert!(grid.is_valid(snap.price()), "{raw} snapped to invalid");
    }
}

#[test]
fn clamping_from_outside_the_grid_is_reported_not_hidden() {
    // A price outside every band means the grid is stale or our reading of it
    // is wrong. The caller must be able to tell that apart from a routine
    // snap, so it can log and count rather than silently accept a fabricated
    // price. Same principle as refusing to truncate excess precision.
    let grid = nonuniform_grid();
    let px = |s: &str| Px::parse_dollars(s).expect("valid");

    let above = grid.snap_to_grid(px("2.0000")).expect("non-empty grid");
    assert_eq!(above, GridSnap::Clamped(px("0.9999")));
    assert!(above.is_clamped());
    assert!(above.was_modified());

    let below = grid.snap_to_grid(px("0.0000")).expect("non-empty grid");
    assert!(
        below.is_clamped(),
        "below the lowest band should clamp, got {below:?}"
    );

    // An exact hit is never reported as modified.
    let exact = grid.snap_to_grid(px("0.4300")).expect("non-empty grid");
    assert!(!exact.was_modified());
    assert!(!exact.is_clamped());
}

#[test]
fn an_empty_grid_invents_nothing() {
    let grid = PriceRanges::default();
    assert!(grid.is_empty());
    assert!(!grid.is_valid(Px::parse_dollars("0.5000").expect("valid")));
    assert_eq!(
        grid.snap_to_grid(Px::parse_dollars("0.5000").expect("valid")),
        None
    );
}

#[test]
fn rejects_structurally_invalid_grids() {
    assert!(PriceRanges::new(vec![band("0.1000", "0.9000", "0.0000")]).is_err());
    assert!(PriceRanges::new(vec![band("0.9000", "0.1000", "0.0100")]).is_err());
    assert!(PriceRanges::new(vec![
        band("0.5000", "0.9000", "0.0100"),
        band("0.1000", "0.4000", "0.0100"),
    ])
    .is_err());
}

#[test]
fn grid_deserializes_from_the_wire_shape() {
    // price_ranges as it arrives in market metadata and in
    // price_level_structure_updated lifecycle messages.
    let json = r#"[
        {"start":"0.0100","end":"0.9900","step":"0.0100"}
    ]"#;
    let grid: PriceRanges = serde_json::from_str(json).expect("deserializes");
    assert_eq!(grid.bands().len(), 1);
    assert!(grid.is_valid(Px::parse_dollars("0.4300").expect("valid")));
    assert!(!grid.is_valid(Px::parse_dollars("0.4350").expect("valid")));
}

// ---------------------------------------------------------------------------
// Ticker and Side
// ---------------------------------------------------------------------------

#[test]
fn ticker_accepts_every_ticker_kalshi_actually_documents() {
    // These are lifted verbatim from asyncapi.yaml's own examples. Three of
    // them contain '.', which the spec's stated pattern ^[A-Z0-9-]+$ would
    // reject -- the pattern is stale and the examples are real. Enforcing the
    // pattern would drop live markets.
    for ticker in [
        "KXNFLGAME-25SEP09-KC",
        "CPI-22DEC-TN0.1",
        "FED-23DEC-T3.00",
        "HIGHNY-22DEC23-B53.5",
        "INXD-23SEP14-B4487",
        "KXBTC-25APR30-T0915-B95000",
        "KXBTC15M-26APR160100-00",
        "KXMVE-TEST-EVENT-M1",
        "TSLA-23DEC-T200",
    ] {
        assert!(
            Ticker::parse(ticker).is_ok(),
            "rejected real ticker {ticker:?}"
        );
    }
}

#[test]
fn ticker_normalizes_case_so_partitions_cannot_collide() {
    // macOS APFS is case-insensitive by default and Linux is not. If both
    // spellings reached the filesystem they would be one directory on the
    // capture host and two on the analysis host -- silently missing data,
    // discovered months later. Normalizing removes the possibility.
    let upper = Ticker::parse("KXNFLGAME-25SEP09-KC").expect("valid");
    let lower = Ticker::parse("kxnflgame-25sep09-kc").expect("valid");
    let mixed = Ticker::parse("KxNflGame-25Sep09-Kc").expect("valid");
    assert_eq!(upper, lower);
    assert_eq!(upper, mixed);
    assert_eq!(upper.as_str(), "KXNFLGAME-25SEP09-KC");

    // The strict constructor surfaces the difference instead of folding it.
    assert!(Ticker::parse_strict("KXNFLGAME-25SEP09-KC").is_ok());
    assert!(Ticker::parse_strict("kxnflgame-25sep09-kc").is_err());

    // And the capture path can tell whether normalization changed anything.
    assert!(Ticker::is_canonical("KXNFLGAME-25SEP09-KC"));
    assert!(!Ticker::is_canonical("kxnflgame-25sep09-kc"));
}

#[test]
fn ticker_round_trips_through_its_partition_path_representation() {
    use std::path::{Path, PathBuf};

    for raw in [
        "KXNFLGAME-25SEP09-KC",
        "CPI-22DEC-TN0.1",
        "HIGHNY-22DEC23-B53.5",
        "KXBTC15M-26APR160100-00",
    ] {
        let ticker = Ticker::parse(raw).expect("valid");

        // Joining onto a data root must produce exactly one new component --
        // no traversal, no split, no absolute-path escape.
        let root = Path::new("data/orderbook_delta");
        let joined: PathBuf = root.join(ticker.as_partition_component());
        assert!(joined.starts_with(root), "{raw} escaped the data root");
        assert_eq!(
            joined.components().count(),
            root.components().count() + 1,
            "{raw} did not produce exactly one path component"
        );

        // And the component reads back as the same ticker.
        let recovered = joined
            .file_name()
            .and_then(|n| n.to_str())
            .expect("utf-8 component");
        assert_eq!(Ticker::parse(recovered).expect("valid"), ticker);
        assert_eq!(recovered, ticker.as_str());
    }
}

#[test]
fn ticker_rejects_what_is_unsafe_as_a_path_component() {
    // Tickers become directory names and Parquet partition keys.
    assert!(Ticker::parse("").is_err());
    assert!(Ticker::parse("..").is_err());
    assert!(Ticker::parse(".").is_err());
    assert!(Ticker::parse("../../etc/passwd").is_err());
    assert!(Ticker::parse(".hidden").is_err());
    assert!(Ticker::parse("A/B").is_err());
    assert!(Ticker::parse("A\\B").is_err());
    assert!(Ticker::parse("A B").is_err());
    assert!(Ticker::parse("A\0B").is_err());
    assert!(Ticker::parse("A\nB").is_err());
    assert!(Ticker::parse(&"A".repeat(129)).is_err());
}

#[test]
fn side_round_trips_through_json() {
    assert_eq!(serde_json::to_string(&Side::Yes).expect("ser"), r#""yes""#);
    assert_eq!(
        serde_json::from_str::<Side>(r#""no""#).expect("de"),
        Side::No
    );
    assert_eq!(Side::Yes.opposite(), Side::No);
}

// ---------------------------------------------------------------------------
// Unit safety
// ---------------------------------------------------------------------------

#[test]
fn scales_are_what_the_docs_say_they_are() {
    assert_eq!(PX_SCALE, 1_000_000, "Px is micro-dollars");
    assert_eq!(
        QTY_SCALE, 100,
        "Qty is the documented _fp scale: 2 decimals"
    );
    assert_eq!(Qty::ONE_CONTRACT.fp_units(), 100);
    assert_eq!(Px::ONE_DOLLAR.micros(), 1_000_000);
}

// ---------------------------------------------------------------------------
// Deserialization must not require a borrowed string
// ---------------------------------------------------------------------------

#[test]
fn fixed_point_types_deserialize_from_owned_and_borrowed_sources() {
    // Regression: an earlier implementation used `<&str>::deserialize`, which
    // parses fine from a &str JSON slice but fails at runtime on any source
    // that cannot lend a borrow -- serde_json::Value, a streaming reader, or a
    // string containing an escape. That shape only appears once real messages
    // flow, so it would have surfaced during capture rather than in tests.
    let json = r#"{"price":"0.5500","size":"10.00","market":"KXNFLGAME-25SEP09-KC"}"#;

    #[derive(serde::Deserialize)]
    struct Row {
        price: Px,
        size: Qty,
        market: Ticker,
    }

    // 1. From a borrowed &str.
    let row: Row = serde_json::from_str(json).expect("from_str");
    assert_eq!(row.price, Px::parse_dollars("0.5500").expect("valid"));
    assert_eq!(row.size, Qty::parse_fp("10.00").expect("valid"));
    assert_eq!(
        row.market,
        Ticker::parse("KXNFLGAME-25SEP09-KC").expect("valid")
    );

    // 2. Via serde_json::Value -- the case that used to fail.
    let value: serde_json::Value = serde_json::from_str(json).expect("to value");
    let row: Row = serde_json::from_value(value).expect("from_value");
    assert_eq!(row.price, Px::parse_dollars("0.5500").expect("valid"));

    // 3. From a streaming reader, which never borrows.
    let row: Row = serde_json::from_reader(json.as_bytes()).expect("from_reader");
    assert_eq!(row.size, Qty::parse_fp("10.00").expect("valid"));

    // 4. From bytes.
    let row: Row = serde_json::from_slice(json.as_bytes()).expect("from_slice");
    assert_eq!(row.price.micros(), 550_000);
}

#[test]
fn price_ranges_deserialize_from_a_value_too() {
    let value = serde_json::json!([
        {"start": "0.0100", "end": "0.9900", "step": "0.0100"}
    ]);
    let grid: PriceRanges = serde_json::from_value(value).expect("from_value");
    assert_eq!(grid.bands().len(), 1);
    assert!(grid.is_valid(Px::parse_dollars("0.4300").expect("valid")));
}
