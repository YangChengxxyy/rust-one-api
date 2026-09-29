//! Tolerant timestamp parsing shared by checkers: RFC3339 strings, unix
//! seconds, unix milliseconds (numbers or numeric strings).

use chrono::{DateTime, Utc};
use serde_json::Value;

pub fn parse_timestamp(v: &Value) -> Option<DateTime<Utc>> {
    match v {
        Value::String(s) => parse_timestamp_str(s),
        Value::Number(n) => n.as_i64().and_then(unix_auto),
        _ => None,
    }
}

pub fn parse_timestamp_str(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    // "YYYY-MM-DD" (treated as start of day UTC)
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0).map(|t| DateTime::from_naive_utc_and_offset(t, Utc));
    }
    s.parse::<i64>().ok().and_then(unix_auto)
}

/// Heuristic: |v| >= 1e12 is milliseconds, else seconds.
pub fn unix_auto(v: i64) -> Option<DateTime<Utc>> {
    if v.abs() >= 1_000_000_000_000 {
        DateTime::from_timestamp_millis(v)
    } else {
        DateTime::from_timestamp(v, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_shapes() {
        let expected = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(parse_timestamp(&Value::String("2026-01-01T00:00:00Z".into())), Some(expected));
        assert_eq!(parse_timestamp(&Value::String("2026-01-01".into())), Some(expected));
        assert_eq!(parse_timestamp(&Value::from(1767225600i64)), Some(expected));
        assert_eq!(parse_timestamp(&Value::from(1767225600000i64)), Some(expected));
        assert_eq!(parse_timestamp(&Value::String("1767225600".into())), Some(expected));
        assert_eq!(parse_timestamp(&Value::Null), None);
    }
}
