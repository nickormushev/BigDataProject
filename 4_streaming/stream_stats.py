
class StreamStats():
    def __init__(self):
        self.count = 0
        self.sum = 0
        self.squared_sum = 0
        self.min = 1
        self.max = 1
        self.mean = 0.0
        self.std = 0.0

    def update(self, value):
        self.count += 1

        self.sum += value
        self.squared_sum += value ** 2
        self.mean = self.sum / self.count

        self.min = min(self.min, value)
        self.max = max(self.max, value)

        self.std = (self.squared_sum / self.count - self.mean ** 2) ** 0.5

    def clear(self):
        self.count = 0
        self.sum = 0
        self.squared_sum = 0
        self.min = 1
        self.max = 1
        self.mean = 0.0
        self.std = 0.0

    def __str__(self):
        return f"mean: {self.mean} +/- {self.std}, min: {self.min}, max: {self.max}"

# Update the stats for each year, month, day, and hour
class StreamDateStats():
    def __init__(self):
        self.year = StreamStats()
        self.month = StreamStats()
        self.day = StreamStats()

        self.year_count = 0
        self.month_count = 0
        self.day_count = 0

        self.total_count = 0

    def increase(self):
        self.year_count += 1
        self.month_count += 1
        self.day_count += 1

        self.total_count += 1

    def reset_year(self):
        self.year.update(self.year_count)
        self.year_count = 0

    def reset_month(self):
        self.month.update(self.month_count)
        self.month_count = 0

    def reset_day(self):
        self.day.update(self.day_count)
        self.day_count = 0

    def clear_day(self):
        self.day.clear()
        self.day_count = 0

    def clear_month(self):
        self.month.clear()
        self.month_count = 0
        
class CounterDateStats():
    def __init__(self):
        self.counter = {}

    def increase(self, key):
        if key in [None, "None", "nan"]:
            return

        if key not in self.counter:
            self.counter[key] = StreamDateStats()

        self.counter[key].increase()

    def items(self):
        return sorted(self.counter.items(), key=lambda x: x[1].total_count, reverse=True)

    def most_commons(self, n):
        return sorted(self.counter.items(), key=lambda x: x[1].total_count, reverse=True)[:n]

    def reset_year(self):
        for key in self.counter:
            self.counter[key].reset_year()

    def reset_month(self):
        for key in self.counter:
            self.counter[key].reset_month()

    def reset_day(self):
        for key in self.counter:
            self.counter[key].reset_day()

    def clear_month(self):
        for key in self.counter:
            self.counter[key].clear_month()

    def clear_day(self):
        for key in self.counter:
            self.counter[key].clear_day()
