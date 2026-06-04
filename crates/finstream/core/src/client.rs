use tokio::task::JoinHandle;

/// A handle to the active financial data stream session.
///
/// `FinStreamClient` manages the lifecycle of all background provider tasks.
/// Dropping the client or calling [`Self::shutdown`] will abort all active connections.
pub struct FinStreamClient {
    pub(crate) handles: Vec<JoinHandle<()>>,
}

impl FinStreamClient {
    /// Aborts all background provider tasks immediately.
    ///
    /// This will close all active WebSocket connections and stop event emission.
    pub fn shutdown(self) {
        // Iterate through all stored join handles and trigger abort
        for handle in self.handles {
            // This immediately terminates the tokio task
            handle.abort();
        }
    }

    /// Waits for all background provider tasks to complete.
    ///
    /// Note that most providers are designed to run indefinitely (reconnecting automatically),
    /// so this method will typically not resolve unless the tasks are externally aborted.
    pub async fn join(self) {
        // Await each task handle in sequence
        for handle in self.handles {
            // Ignore results/errors during join as handles might have been aborted
            let _ = handle.await;
        }
    }
}
