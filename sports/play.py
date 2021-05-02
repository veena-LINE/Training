import datetime as dt
from games import Game
from players import Player

game = Game("NFL", dt.datetime.now())
print(game)

player = Player("Veena", 0)
print(player)
