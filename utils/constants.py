from datetime import date

# First date of the historical backfill. Every place is ingested from this date onwards so
# long term averages and other computed values have a consistent history to work from.
BACKFILL_START_DATE: date = date(1990, 1, 1)
