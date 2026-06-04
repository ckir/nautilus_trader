use std::time::Duration;

/// Defines the strategy for automatically reconnecting to a provider after a failure.
///
/// The policy uses exponential backoff with optional random jitter and supports
/// limits based on either the number of attempts or total elapsed time.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Stop retrying after this many attempts. `None` = no limit.
    pub max_retries:  Option<u32>,
    /// Stop retrying after this wall-clock duration. `None` = no limit.
    pub max_duration: Option<Duration>,
    /// The base delay for the first reconnection attempt.
    pub initial_delay: Duration,
    /// The absolute maximum delay allowed between retries.
    pub max_delay:     Duration,
    /// Whether to apply random jitter (0.5x to 1.0x) to the computed delay.
    pub jitter:        bool,
}

impl Default for ReconnectPolicy {
    /// Returns a standard policy: 1s initial, 60s max, jitter enabled, 1-hour total limit.
    fn default() -> Self {
        Self {
            max_retries:   None,
            max_duration:  Some(Duration::from_secs(3600)),
            initial_delay: Duration::from_secs(1),
            max_delay:     Duration::from_secs(60),
            jitter:        true,
        }
    }
}

impl ReconnectPolicy {
    /// Computes the sleep duration for the given attempt number (0-based).
    ///
    /// The delay grows exponentially (2^attempt) from the initial delay
    /// and is capped by the maximum delay.
    pub fn next_delay(&self, attempt: u32) -> Duration {
        // Calculate the exponential backoff: base * 2^attempt
        let base_secs = self.initial_delay.as_secs_f64()
            * 2_f64.powi(attempt as i32);
        
        // Ensure the delay does not exceed the configured maximum
        let capped = base_secs.min(self.max_delay.as_secs_f64());

        // Apply random jitter if enabled to prevent thundering herds
        let final_secs = if self.jitter {
            // Scale by a factor between 0.5 and 1.0
            let factor: f64 = rand::random_range(0.5..=1.0);
            capped * factor
        } else {
            capped
        };

        // Return duration, ensuring at least 100ms to avoid tight loops
        Duration::from_secs_f64(final_secs.max(0.1))
    }

    /// Returns `true` if the retry limit has been reached by either criterion.
    ///
    /// * `attempt`: The number of failed attempts so far (0-based before first retry).
    /// * `elapsed`: The wall-clock time since the first failure.
    pub fn is_exceeded(&self, attempt: u32, elapsed: Duration) -> bool {
        // Check if retry count limit is reached
        if let Some(max) = self.max_retries {
            if attempt >= max {
                return true;
            }
        }
        // Check if total wall-clock time limit is reached
        if let Some(max_dur) = self.max_duration {
            if elapsed >= max_dur {
                return true;
            }
        }
        // Neither limit reached
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_and_caps() {
        let policy = ReconnectPolicy {
            max_retries:   Some(10),
            max_duration:  None,
            initial_delay: Duration::from_secs(1),
            max_delay:     Duration::from_secs(60),
            jitter:        false,
        };

        assert_eq!(policy.next_delay(0), Duration::from_secs(1));
        assert_eq!(policy.next_delay(1), Duration::from_secs(2));
        assert_eq!(policy.next_delay(2), Duration::from_secs(4));
        assert_eq!(policy.next_delay(10), Duration::from_secs(60));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let policy = ReconnectPolicy {
            max_retries:   None,
            max_duration:  None,
            initial_delay: Duration::from_secs(2),
            max_delay:     Duration::from_secs(60),
            jitter:        true,
        };

        for _ in 0..100 {
            let d = policy.next_delay(3); // base = 16s
            assert!(d >= Duration::from_secs(7));
            assert!(d <= Duration::from_secs(17));
        }
    }

    #[test]
    fn is_exceeded_by_count() {
        let policy = ReconnectPolicy {
            max_retries:   Some(3),
            max_duration:  None,
            ..Default::default()
        };
        assert!(!policy.is_exceeded(2, Duration::from_secs(0)));
        assert!(policy.is_exceeded(3, Duration::from_secs(0)));
    }

    #[test]
    fn is_exceeded_by_duration() {
        let policy = ReconnectPolicy {
            max_retries:   None,
            max_duration:  Some(Duration::from_secs(60)),
            ..Default::default()
        };
        assert!(!policy.is_exceeded(99, Duration::from_secs(59)));
        assert!(policy.is_exceeded(99, Duration::from_secs(60)));
    }
}
