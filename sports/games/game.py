

class Game:
    def __init__(self, name, date):
        self.name = name
        self.date = date

    def __str__(self):
        return f"{self.name}/{self.date}"

    def __repr__(self):
        return self.__str__()

