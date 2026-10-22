//! Exponential backoff with full jitter, capped.
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub base: Duration,
    pub max: Duration,
}

impl Backoff {
    /// Upper bound of the delay before attempt `n` (1-based count of failures so far).
    pub fn ceiling(&self, failures: u32) -> Duration {
        let exp = failures.saturating_sub(1).min(30);
        self.base.saturating_mul(1u32 << exp).min(self.max)
    }

    /// Full-jitter delay in `[ceiling/2, ceiling]` to avoid thundering herds while keeping progress.
    pub fn delay(&self, failures: u32) -> Duration {
        let c = self.ceiling(failures).as_millis() as u64;
        let lo = c / 2;
        Duration::from_millis(if c > lo {
            rand::random::<u64>() % (c - lo + 1) + lo
        } else {
            c
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially_and_caps() {
        let b = Backoff {
            base: Duration::from_secs(1),
            max: Duration::from_secs(60),
        };
        assert_eq!(b.ceiling(1), Duration::from_secs(1));
        assert_eq!(b.ceiling(2), Duration::from_secs(2));
        assert_eq!(b.ceiling(4), Duration::from_secs(8));
        assert_eq!(b.ceiling(10), Duration::from_secs(60));
        assert_eq!(b.ceiling(1000), Duration::from_secs(60));
    }

    #[test]
    fn jitter_stays_in_bounds() {
        let b = Backoff {
            base: Duration::from_millis(100),
            max: Duration::from_secs(10),
        };
        for f in 1..12 {
            let d = b.delay(f);
            assert!(d <= b.ceiling(f) && d >= b.ceiling(f) / 2);
        }
    }
}
