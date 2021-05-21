

class Player:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __str__(self):
        return f"{self.name}/{self.age}"

    def __repr__(self):
        return self.__str__()

