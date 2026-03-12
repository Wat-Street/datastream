from calendars.definitions import EverydayCalendar, WeekdayCalendar
from calendars.interface import Calendar

# registry mapping calendar name -> Calendar instance
CALENDARS_MAP: dict[str, Calendar] = {
    "everyday": EverydayCalendar(),
    "weekday": WeekdayCalendar(),
}
