use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use input_event::{Event, KeyboardEvent};
use std::cell::RefCell;

thread_local! {
    static KEY_SEQ: RefCell<HashMap<u32, u64>> = RefCell::new(HashMap::new());
    static PRESS_TIMES: RefCell<HashMap<u32, (u128, u32, u64)>> = RefCell::new(HashMap::new());
}

pub use self::error::{EmulationCreationError, EmulationError, InputEmulationError};

#[cfg(windows)]
mod windows;

#[cfg(x11)]
mod x11;

#[cfg(wlroots)]
mod wlroots;

#[cfg(rdp)]
mod xdg_desktop_portal;

#[cfg(libei)]
mod libei;

#[cfg(target_os = "macos")]
mod macos;

/// fallback input emulation (logs events)
mod dummy;
mod error;

pub type EmulationHandle = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    #[cfg(wlroots)]
    Wlroots,
    #[cfg(libei)]
    Libei,
    #[cfg(rdp)]
    Xdp,
    #[cfg(x11)]
    X11,
    #[cfg(windows)]
    Windows,
    #[cfg(target_os = "macos")]
    MacOs,
    Dummy,
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(wlroots)]
            Backend::Wlroots => write!(f, "wlroots"),
            #[cfg(libei)]
            Backend::Libei => write!(f, "libei"),
            #[cfg(rdp)]
            Backend::Xdp => write!(f, "xdg-desktop-portal"),
            #[cfg(x11)]
            Backend::X11 => write!(f, "X11"),
            #[cfg(windows)]
            Backend::Windows => write!(f, "windows"),
            #[cfg(target_os = "macos")]
            Backend::MacOs => write!(f, "macos"),
            Backend::Dummy => write!(f, "dummy"),
        }
    }
}

pub struct DelayedEvent {
    pub event: Event,
    pub handle: EmulationHandle,
    pub press_id: u64,
}

pub struct InputEmulation {
    emulation: Box<dyn Emulation>,
    handles: HashSet<EmulationHandle>,
    pressed_keys: HashMap<EmulationHandle, HashSet<u32>>,
    delayed_tx: tokio::sync::mpsc::UnboundedSender<DelayedEvent>,
    delayed_rx: tokio::sync::mpsc::UnboundedReceiver<DelayedEvent>,
}

impl InputEmulation {
    async fn with_backend(backend: Backend) -> Result<InputEmulation, EmulationCreationError> {
        let emulation: Box<dyn Emulation> = match backend {
            #[cfg(wlroots)]
            Backend::Wlroots => Box::new(wlroots::WlrootsEmulation::new()?),
            #[cfg(libei)]
            Backend::Libei => Box::new(libei::LibeiEmulation::new().await?),
            #[cfg(x11)]
            Backend::X11 => Box::new(x11::X11Emulation::new()?),
            #[cfg(rdp)]
            Backend::Xdp => Box::new(xdg_desktop_portal::DesktopPortalEmulation::new().await?),
            #[cfg(windows)]
            Backend::Windows => Box::new(windows::WindowsEmulation::new()?),
            #[cfg(target_os = "macos")]
            Backend::MacOs => Box::new(macos::MacOSEmulation::new()?),
            Backend::Dummy => Box::new(dummy::DummyEmulation::new()),
        };
        let (delayed_tx, delayed_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            emulation,
            handles: HashSet::new(),
            pressed_keys: HashMap::new(),
            delayed_tx,
            delayed_rx,
        })
    }

    pub async fn new(backend: Option<Backend>) -> Result<InputEmulation, EmulationCreationError> {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let sys_time_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                PRESS_TIMES.with(|m| {
                    for (&key, &(press_time, _, _)) in m.borrow().iter() {
                        if sys_time_ms.saturating_sub(press_time) > 1000 {
                            log::debug!(
                                "key {} has been PRESSED for {} ms with NO RELEASE!",
                                key,
                                sys_time_ms.saturating_sub(press_time)
                            );
                        }
                    }
                });
            }
        });

        if let Some(backend) = backend {
            let b = Self::with_backend(backend).await;
            if b.is_ok() {
                log::info!("using emulation backend: {backend}");
            }
            return b;
        }

        for backend in [
            #[cfg(wlroots)]
            Backend::Wlroots,
            #[cfg(libei)]
            Backend::Libei,
            #[cfg(rdp)]
            Backend::Xdp,
            #[cfg(x11)]
            Backend::X11,
            #[cfg(windows)]
            Backend::Windows,
            #[cfg(target_os = "macos")]
            Backend::MacOs,
            Backend::Dummy,
        ] {
            match Self::with_backend(backend).await {
                Ok(b) => {
                    log::info!("using emulation backend: {backend}");
                    return Ok(b);
                }
                Err(e) if e.cancelled_by_user() => return Err(e),
                Err(e) => log::warn!("{e}"),
            }
        }

        Err(EmulationCreationError::NoAvailableBackend)
    }

    pub async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        match event {
            Event::Keyboard(KeyboardEvent::Key { time, key, state }) => {
                let _previously_pressed = self.has_pressed_keys(handle);
                let allowed = self.update_pressed_keys(handle, key, state);

                let sys_time_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let seq = KEY_SEQ.with(|m| {
                    let mut m = m.borrow_mut();
                    if state == 1 {
                        let e = m.entry(key).or_insert(0);
                        *e += 1;
                        *e
                    } else {
                        *m.get(&key).unwrap_or(&0)
                    }
                });

                let mut is_delayed = false;
                if state == 1 {
                    PRESS_TIMES.with(|m| {
                        let mut m = m.borrow_mut();
                        if let Some(entry) = m.get_mut(&key) {
                            entry.2 = seq; // update press_id
                        } else {
                            m.insert(key, (sys_time_ms, time, seq));
                        }
                    });
                }

                if state == 0 && allowed {
                    if let Some((recv_press_time, source_press_time, press_id)) =
                        PRESS_TIMES.with(|m| m.borrow().get(&key).copied())
                    {
                        let recv_duration = sys_time_ms.saturating_sub(recv_press_time) as i64;
                        let source_duration = time.wrapping_sub(source_press_time) as i64;
                        let receiver_delay = recv_duration - source_duration;

                        if receiver_delay < 0 {
                            // Release is early, we must delay it!
                            is_delayed = true;
                            let delay_ms = (-receiver_delay) as u64;
                            let tx = self.delayed_tx.clone();
                            let delayed_event = DelayedEvent {
                                event,
                                handle,
                                press_id,
                            };
                            tokio::task::spawn_local(async move {
                                tokio::time::sleep(std::time::Duration::from_millis(delay_ms))
                                    .await;
                                let _ = tx.send(delayed_event);
                            });
                        } else {
                            // Not delayed, remove it from PRESS_TIMES
                            PRESS_TIMES.with(|m| m.borrow_mut().remove(&key));
                        }
                    }
                }

                // prevent double pressed / released keys
                if allowed && !is_delayed {
                    self.emulation.consume(event, handle).await?;
                } else if allowed && is_delayed {
                    // Do NOT pass to backend yet, but we MUST keep it in pressed_keys
                    // But `update_pressed_keys` already removed it because state == 0!
                    // We must put it back!
                    self.pressed_keys.entry(handle).or_default().insert(key);
                }
                Ok(())
            }
            _ => self.emulation.consume(event, handle).await,
        }
    }

    pub async fn next_delayed(&mut self) -> Option<DelayedEvent> {
        self.delayed_rx.recv().await
    }

    pub async fn consume_delayed(&mut self, delayed: DelayedEvent) -> Result<(), EmulationError> {
        if let Event::Keyboard(KeyboardEvent::Key { key, .. }) = delayed.event {
            let mut valid = false;
            PRESS_TIMES.with(|m| {
                if let Some(&(_, _, current_press_id)) = m.borrow().get(&key) {
                    if current_press_id == delayed.press_id {
                        valid = true;
                    }
                }
            });
            if valid {
                // Perform the actual release now.
                PRESS_TIMES.with(|m| m.borrow_mut().remove(&key));
                if let Some(keys) = self.pressed_keys.get_mut(&delayed.handle) {
                    keys.remove(&key);
                }
                self.emulation
                    .consume(delayed.event, delayed.handle)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn create(&mut self, handle: EmulationHandle) -> bool {
        if self.handles.insert(handle) {
            self.pressed_keys.insert(handle, HashSet::new());
            self.emulation.create(handle).await;
            true
        } else {
            false
        }
    }

    pub async fn destroy(&mut self, handle: EmulationHandle) {
        let _ = self.release_keys(handle).await;
        if self.handles.remove(&handle) {
            self.pressed_keys.remove(&handle);
            self.emulation.destroy(handle).await
        }
    }

    pub async fn terminate(&mut self) {
        for handle in self.handles.iter().cloned().collect::<Vec<_>>() {
            self.destroy(handle).await
        }
        self.emulation.terminate().await
    }

    pub async fn release_keys(&mut self, handle: EmulationHandle) -> Result<(), EmulationError> {
        if let Some(keys) = self.pressed_keys.get_mut(&handle) {
            let keys = keys.drain().collect::<Vec<_>>();
            for key in keys {
                let event = Event::Keyboard(KeyboardEvent::Key {
                    time: 0,
                    key,
                    state: 0,
                });
                self.emulation.consume(event, handle).await?;
                if let Ok(key) = input_event::scancode::Linux::try_from(key) {
                    log::warn!("releasing stuck key: {key:?}");
                }
            }
        }

        let event = Event::Keyboard(KeyboardEvent::Modifiers {
            depressed: 0,
            latched: 0,
            locked: 0,
            group: 0,
        });
        self.emulation.consume(event, handle).await?;
        Ok(())
    }

    pub fn has_pressed_keys(&self, handle: EmulationHandle) -> bool {
        self.pressed_keys
            .get(&handle)
            .is_some_and(|p| !p.is_empty())
    }

    /// update the pressed_keys for the given handle
    /// returns whether the event should be processed
    fn update_pressed_keys(&mut self, handle: EmulationHandle, key: u32, state: u8) -> bool {
        let Some(pressed_keys) = self.pressed_keys.get_mut(&handle) else {
            return false;
        };

        if state == 0 {
            // currently pressed => can release
            pressed_keys.remove(&key)
        } else {
            // currently not pressed => can press
            pressed_keys.insert(key)
        }
    }
}

#[async_trait]
trait Emulation: Send {
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError>;
    async fn create(&mut self, handle: EmulationHandle);
    async fn destroy(&mut self, handle: EmulationHandle);
    async fn terminate(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;
    use input_event::{Event, KeyboardEvent};

    struct MockEmulation {
        emitted_events: std::cell::RefCell<Vec<Event>>,
    }

    impl MockEmulation {
        fn new() -> Self {
            Self {
                emitted_events: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Emulation for MockEmulation {
        async fn create(&mut self, _handle: EmulationHandle) {}
        async fn destroy(&mut self, _handle: EmulationHandle) {}
        async fn terminate(&mut self) {}
        async fn consume(
            &mut self,
            event: Event,
            _handle: EmulationHandle,
        ) -> Result<(), EmulationError> {
            self.emitted_events.borrow_mut().push(event);
            Ok(())
        }
    }

    fn key_event(time: u32, key: u32, state: u8) -> Event {
        Event::Keyboard(KeyboardEvent::Key { time, key, state })
    }

    async fn setup_emulation() -> InputEmulation {
        let (delayed_tx, delayed_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut emul = InputEmulation {
            emulation: Box::new(MockEmulation::new()),
            handles: HashSet::new(),
            pressed_keys: HashMap::new(),
            delayed_tx,
            delayed_rx,
        };
        emul.create(1).await;
        emul
    }

    #[tokio::test]
    async fn test_scheduler_race_repress() {
        tokio::task::LocalSet::new()
            .run_until(async move {
                let mut e = setup_emulation().await;

                // A DOWN
                let _ = e.consume(key_event(1000, 30, 1), 1).await;
                // A UP (early) -> scheduled for +100ms
                let _ = e.consume(key_event(1100, 30, 0), 1).await;

                // Ensure it's delayed!
                let delayed = e.next_delayed().await.unwrap();
                assert_eq!(delayed.press_id, 1);

                // 50ms later: A DOWN
                let _ = e.consume(key_event(1150, 30, 1), 1).await;

                // 50ms later: old A UP fires
                let _ = e.consume_delayed(delayed).await;

                // Expected: old A UP discarded, A remains pressed!
                assert!(e.has_pressed_keys(1));
            })
            .await;
    }
}
