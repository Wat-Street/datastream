from calendars.interface import Calendar

# registry mapping calendar name -> Calendar instance
CALENDARS_MAP: dict[str, Calendar] = {}
