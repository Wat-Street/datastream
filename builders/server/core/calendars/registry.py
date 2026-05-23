from core.calendars.definitions.always_open import AlwaysOpenCalendar
from core.calendars.definitions.everyday import EverydayCalendar
from core.calendars.definitions.nyse_daily import NyseDailyCalendar
from core.calendars.definitions.weekday import WeekdayCalendar
from core.calendars.interface import Calendar

# registry mapping calendar name -> Calendar instance
CALENDARS_MAP: dict[str, Calendar] = {
    "everyday": EverydayCalendar(),
    "weekday": WeekdayCalendar(),
    "always-open": AlwaysOpenCalendar(),
    "nyse-daily": NyseDailyCalendar(),
}
