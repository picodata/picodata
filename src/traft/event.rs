use std::borrow::Borrow;
use std::collections::{HashMap, LinkedList};
use std::fmt::Write;
use std::rc::Rc;
use std::str::FromStr;
use std::time::{Duration, Instant};

use ::tarantool::fiber::{mutex::MutexGuard, Cond, Mutex};
use ::tarantool::proc;
use ::tarantool::unwrap_or;

use crate::tlog;
use crate::traft::error::Error;
use crate::traft::Result;
use crate::unwrap_ok_or;

pub type BoxResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

::tarantool::define_str_enum! {
    ////////////////////////////////////////////////////////////////////////////
    /// An enumeration of builtin events
    pub enum Event {
        JointStateEnter = "raft.joint-state-enter",
        JointStateLeave = "raft.joint-state-leave",
        JointStateDrop = "raft.joint-state-drop",
        EntryApplied = "raft.entry-applied",
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Struct that stores information about event handlers
#[derive(Default)]
pub struct Events {
    handlers: HashMap<Event, Handler>,
    conds: HashMap<Event, Rc<Cond>>,
}

impl Events {
    // TODO: query/modify registered handlers
    fn once_handler(&mut self, event: &Event) -> Option<Handler> {
        self.handlers.remove(event)
    }

    #[allow(dead_code)]
    fn add_once_handler(&mut self, event: Event, handler: Handler) {
        self.handlers.insert(event, handler);
    }

    /// Signals to everybody who's waiting for this repeated `event`.
    ///
    /// **does not yield**
    fn broadcast_repeated(&self, event: &Event) {
        if let Some(cond) = self.conds.get(event) {
            cond.broadcast()
        }
    }

    /// Returns a [`Cond`] which will be signalled every time the given `event`
    /// occurs. Can be used to wait for a repeated event.
    ///
    /// [`Events`] should always be accessed via [`MutexGuard`], therefore this
    /// function returns a `Rc<Cond>` rather then waiting on it, so that the
    /// mutex guard can be released before the fiber yields.
    ///
    /// **does not yield**
    fn regular_cond(&mut self, event: Event) -> Rc<Cond> {
        self.conds
            .entry(event)
            .or_insert_with(|| Rc::new(Cond::new()))
            .clone()
    }
}

/// Result returned from [`wait_timeout`]. Specifies whether the call resulted
/// in a signal or a timeout.
pub enum WaitTimeout {
    /// Event was signaled.
    Signal,
    /// Timeout exceeded before event could be signaled.
    Timeout,
}

impl WaitTimeout {
    #[inline]
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }
}

////////////////////////////////////////////////////////////////////////////////
// functions

/// Waits for the event to happen or timeout to end.
///
/// Returns an error if the `EVENTS` is uninitialized.
pub fn wait_timeout(event: Event, timeout: Duration) -> Result<WaitTimeout> {
    let mut events = events()?;
    let cond = events.regular_cond(event);
    // events must be released before yielding
    drop(events);
    Ok(if cond.wait_timeout(timeout) {
        WaitTimeout::Signal
    } else {
        WaitTimeout::Timeout
    })
}

/// Waits for the event to happen or deadline to be reached.
///
/// Returns an error if the `EVENTS` is uninitialized.
pub fn wait_deadline(event: Event, deadline: Instant) -> Result<WaitTimeout> {
    let mut events = events()?;
    let cond = events.regular_cond(event);
    // events must be released before yielding
    drop(events);
    let timeout = deadline.saturating_duration_since(Instant::now());
    Ok(if cond.wait_timeout(timeout) {
        WaitTimeout::Signal
    } else {
        WaitTimeout::Timeout
    })
}

/// Signals to everybody who's waiting for this `event` either repeated or one
/// time.
///
/// If `EVENTS` is uninitialized, nothing happens
pub fn broadcast(event: impl Borrow<Event>) {
    let event = event.borrow();
    tlog!(Debug, "broadcast"; "event" => event.as_str());
    let mut events = unwrap_ok_or!(events(), Err(_) => return);
    events.broadcast_repeated(event);
    let handler = unwrap_or!(events.once_handler(event), return);
    if let Err(e) = handler.handle() {
        tlog!(Warning, "error happened during handling of event: {e}";
            "event" => event.as_str(),
        )
    }
}

/// Sets the `target` event to be broadcast when the `when` event happens.
///
/// **NOTE**: the postponement is volatile, so if the instance restarts between
/// the `target` and the `when` events happen, there will not be a
/// notification.
///
/// Adds an event handler which will broadcast the `target` event when the
/// `when` event happens.
///
/// Returns an error if `EVENTS` is uninitialized
pub fn broadcast_when(target: Event, when: Event) -> Result<()> {
    let mut events = events()?;
    let cond = events.regular_cond(target);
    events.add_once_handler(
        when,
        handler(move || {
            tlog!(Debug, "broadcast"; "event" => target.as_str());
            cond.broadcast();
            Ok(())
        }),
    );
    // events must be released before yielding
    drop(events);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
/// Struct that handles an event
pub struct Handler {
    // TODO: add ability to pass context to event handler
    pub cbs: LinkedList<Box<dyn FnOnce() -> BoxResult<()>>>,
}

impl Handler {
    fn new<T>(cb: T) -> Self
    where
        T: 'static,
        T: FnOnce() -> BoxResult<()>,
    {
        let mut cbs: LinkedList<Box<dyn FnOnce() -> BoxResult<()>>> = LinkedList::new();
        cbs.push_back(Box::new(cb));
        Self { cbs }
    }

    #[allow(dead_code)]
    /// Add a callback to this event handler
    pub fn push<T>(&mut self, cb: T)
    where
        T: 'static,
        T: FnOnce() -> BoxResult<()>,
    {
        self.cbs.push_back(Box::new(cb));
    }

    /// Handle the event.
    pub fn handle(self) -> BoxResult<()> {
        let (_, errs): (Vec<_>, Vec<_>) = self
            .cbs
            .into_iter()
            .map(|cb| (cb)())
            .partition(|res| res.is_ok());
        match &errs[..] {
            [] => Ok(()),
            [_only_one_error] => errs.into_iter().next().unwrap(),
            [..] => {
                let mut msg = String::with_capacity(128);
                writeln!(msg, "{} errors happened:", errs.len()).unwrap();
                for err in errs {
                    writeln!(msg, "{}", err.unwrap_err()).unwrap();
                }
                Err(msg.into())
            }
        }
    }
}

#[allow(dead_code)]
pub fn handler<T>(cb: T) -> Handler
where
    T: 'static,
    T: FnOnce() -> BoxResult<()>,
{
    Handler::new(cb)
}

////////////////////////////////////////////////////////////////////////////////
/// Global [`Events`] instance that handles all events received by the instance
static mut EVENTS: Option<Box<Mutex<Events>>> = None;

/// Initialize the global [`Events`] singleton. **Should only be called once**
pub fn init() {
    unsafe {
        assert!(EVENTS.is_none(), "event::init() must be called only once");
        EVENTS = Some(Box::new(Mutex::new(Events::default())));
    }
}

/// Acquire the global [`Events`] singleton.
pub fn events() -> Result<MutexGuard<'static, Events>> {
    if let Some(events) = unsafe { EVENTS.as_ref() } {
        Ok(events.lock())
    } else {
        Err(Error::EventsUninitialized)
    }
}

////////////////////////////////////////////////////////////////////////////////
// proc
#[proc]
fn raft_event(event: String) -> BoxResult<()> {
    let event = Event::from_str(&event).map_err(|e| format!("{e}: {event}"))?;

    let handler = events()?
        .once_handler(&event)
        .ok_or_else(|| format!("no handler registered for '{event}'"))?;
    handler.handle()
}
