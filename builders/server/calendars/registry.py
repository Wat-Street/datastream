from calendars.definitions.always_open import AlwaysOpenCalendar
from calendars.definitions.everyday import EverydayCalendar
from calendars.definitions.nyse_daily import NyseDailyCalendar
from calendars.definitions.weekday import WeekdayCalendar
from calendars.interface import Calendar

# registry mapping calendar name -> Calendar instance
CALENDARS_MAP: dict[str, Calendar] = {
    "everyday": EverydayCalendar(),
    "weekday": WeekdayCalendar(),
    "always-open": AlwaysOpenCalendar(),
    "nyse-daily": NyseDailyCalendar(),
}
