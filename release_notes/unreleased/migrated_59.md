## fix!

- `CancellationTokenHandle::cancel()` now returns `Rc<OnceEvent>` instead of
  `Channel<()>`. Use `finish_event.is_finished()` to check completion or
  `finish_event.wait_timeout(duration)` to wait with a timeout.
