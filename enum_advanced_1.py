from enum import Enum, auto

class Color(Enum):
  RED = auto()
  GREEN = auto()
  BLUE = auto()

# Accessing enum values
print(Color.RED)  # Output: Color.RED

# Enum iteration
for color in Color:
  print(color)  # Output: Color.RED, Color.GREEN, Color.BLUE

# Enum comparison
print(Color.RED == Color.GREEN)  # Output: False

# Enum value lookup
print(Color['GREEN'])  # Output: Color.GREEN

# Enum value access
print(Color.RED.value)  # Output: 1