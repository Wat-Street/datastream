# Calendars (`core/calendars`)

Calendars determine which timestamps are valid for a dataset. Each dataset declares a `calendar`
in `config.toml`. At build time, `generate_timestamps()` (`core/service/timestamps.py`) filters
candidate timestamps through the calendar's `is_open()`, so only valid dates are built and stored.
The worker passes `cfg.calendar` automatically when building each job.

```
core/calendars/
  interface.py      Calendar ABC
  utils.py          utility functions
  definitions/      concrete calendar implementations
  registry.py       CALENDARS_MAP registry
```

## Calendar interface

The `Calendar` ABC (`interface.py`):
- `name: str`: unique identifier (abstract property).
- `granularity: timedelta`: smallest time step (abstract property).
- `is_open(timestamp) -> bool`: whether a timestamp is valid (abstract method).
- `next_open(timestamp) -> datetime | None`: next valid datetime >= timestamp, or None if never
  open again (abstract method).

Calendars are lightweight; they may hold state but should stay minimal.

## Available calendars

Registered in `CALENDARS_MAP` (`registry.py`):
- `everyday`: every day is valid (`is_open` always `True`). The default.
- `weekday`: Monday through Friday valid, weekends not.
- `nyse-daily`: NYSE trading days only (excludes weekends and NYSE holidays), via the
  `exchange_calendars` XNYS calendar.

## Config integration

- `DatasetConfig.calendar` is a `Calendar` instance, not a string.
- During config loading the calendar string is validated against `CALENDARS_MAP` and resolved to
  an instance. The field is required; missing or unknown names raise a `ValueError`.

## Future work

- Cross-dataset dependency lookups may use as-of semantics (nearest prior valid timestamp) to
  handle calendar mismatches.
- Additional calendars can be added by subclassing `Calendar` and registering in `CALENDARS_MAP`.
