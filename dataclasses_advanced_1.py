"""
dataclass_advaned - Advanced usage of dataclasses
"""
from dataclasses import dataclass, field
from typing import List

@dataclass
class Person:
  name: str
  age: int
  hobbies: List[str] = field(default_factory=list)
  address: str = ""

  def add_hobby(self, hobby: str):
    self.hobbies.append(hobby)

  def __post_init__(self):
    if not self.address:
      self.address = "Unknown"

# Create instances of Person
person1 = Person("Alice", 25, ["Reading", "Painting"], "123 Main St")
person2 = Person("Bob", 30)

# Add a new hobby to person2
person2.add_hobby("Gardening")

# Print the details of the persons
print(person1)
print(person2)