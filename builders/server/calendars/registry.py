from calendars.definitions import AlwaysOpenCalendar, EverydayCalendar, WeekdayCalendar
from calendars.interface import Calendar

# registry mapping calendar name -> Calendar instance
CALENDARS_MAP: dict[str, Calendar] = {
    "everyday": EverydayCalendar(),
    "weekday": WeekdayCalendar(),
    "always-open": AlwaysOpenCalendar(),
}
