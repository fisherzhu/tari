use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SelectedRoute {
    pub(super) index: usize,
    revision: u64,
}

#[derive(Debug, Default)]
struct RouteState {
    current: Option<SelectedRoute>,
    last_index: Option<usize>,
    revision: u64,
}

impl RouteState {
    fn select(&mut self, index: usize) -> SelectedRoute {
        self.revision += 1;
        let route = SelectedRoute {
            index,
            revision: self.revision,
        };
        self.current = Some(route);
        self.last_index = Some(index);
        route
    }

    fn invalidate(&mut self, route: SelectedRoute) -> bool {
        if self.current != Some(route) {
            return false;
        }
        self.revision += 1;
        self.current = None;
        true
    }

    fn promote(&mut self, backup: SelectedRoute) -> Option<SelectedRoute> {
        if backup.index == 0 || self.current != Some(backup) {
            return None;
        }
        Some(self.select(0))
    }
}

#[derive(Debug, Default)]
pub(super) struct MonerodRoutes {
    state: Mutex<RouteState>,
    pub(super) qualification: tokio::sync::Mutex<()>,
    instance: u64,
}

impl MonerodRoutes {
    pub(super) fn new() -> Self {
        Self {
            instance: rand::random(),
            ..Self::default()
        }
    }

    pub(super) fn current(&self) -> Option<SelectedRoute> {
        self.state.lock().expect("monerod route lock poisoned").current
    }

    pub(super) fn last_index(&self) -> Option<usize> {
        self.state.lock().expect("monerod route lock poisoned").last_index
    }

    pub(super) fn select(&self, index: usize) -> SelectedRoute {
        self.state.lock().expect("monerod route lock poisoned").select(index)
    }

    pub(super) fn invalidate(&self, route: SelectedRoute) -> bool {
        self.state
            .lock()
            .expect("monerod route lock poisoned")
            .invalidate(route)
    }

    pub(super) fn promote(&self, backup: SelectedRoute) -> Option<SelectedRoute> {
        self.state.lock().expect("monerod route lock poisoned").promote(backup)
    }

    pub(super) fn observation(&self) -> Value {
        let state = self.state.lock().expect("monerod route lock poisoned");
        let (phase, upstream) = match state.current {
            Some(route) if route.index == 0 => ("selected", "primary"),
            Some(_) => ("selected", "backup"),
            None if self.qualification.try_lock().is_err() => ("qualifying", "unknown"),
            None => ("unselected", "unknown"),
        };
        let observed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        json!({
            "schema": 1,
            "instance": format!("{:016x}", self.instance),
            "revision": state.revision,
            "phase": phase,
            "upstream": upstream,
            "observedAtMs": u64::try_from(observed_at).unwrap_or(u64::MAX),
        })
    }
}

impl SelectedRoute {
    pub(super) fn revision(self) -> u64 {
        self.revision
    }
}

#[derive(Default)]
pub(super) struct PrimaryRecovery {
    streak: Option<SelectedRoute>,
}

impl PrimaryRecovery {
    pub(super) fn observe(&mut self, backup: SelectedRoute, healthy: bool) -> bool {
        if !healthy {
            self.streak = None;
            return false;
        }
        if self.streak == Some(backup) {
            self.streak = None;
            return true;
        }
        self.streak = Some(backup);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_failure_cannot_clear_a_reselected_endpoint() {
        let mut state = RouteState::default();
        let old_primary = state.select(0);
        assert!(state.invalidate(old_primary));
        let backup = state.select(1);
        let new_primary = state.promote(backup).unwrap();
        assert!(!state.invalidate(old_primary));
        assert!(!state.invalidate(backup));
        assert_eq!(state.current, Some(new_primary));
    }

    #[test]
    fn stale_probe_cannot_promote_a_reselected_backup() {
        let mut state = RouteState::default();
        let old_backup = state.select(1);
        assert!(state.invalidate(old_backup));
        let new_backup = state.select(1);
        assert!(state.promote(old_backup).is_none());
        assert_eq!(state.current, Some(new_backup));
    }

    #[test]
    fn primary_recovery_requires_consecutive_successes_on_the_same_revision() {
        let mut state = RouteState::default();
        let mut recovery = PrimaryRecovery::default();
        let backup = state.select(1);
        assert!(!recovery.observe(backup, true));
        assert!(!recovery.observe(backup, false));
        assert!(!recovery.observe(backup, true));
        let new_backup = state.select(1);
        assert!(!recovery.observe(new_backup, true));
        assert!(recovery.observe(new_backup, true));
    }

    #[tokio::test]
    async fn dropping_qualification_guard_releases_waiters() {
        let routes = MonerodRoutes::new();
        assert_eq!(routes.observation()["phase"], "unselected");
        let guard = routes.qualification.lock().await;
        assert_eq!(routes.observation()["phase"], "qualifying");
        drop(guard);
        assert!(routes.qualification.try_lock().is_ok());
        routes.select(1);
        let observation = routes.observation();
        assert_eq!(observation["phase"], "selected");
        assert_eq!(observation["upstream"], "backup");
        assert_eq!(observation["revision"], 1);
    }
}
