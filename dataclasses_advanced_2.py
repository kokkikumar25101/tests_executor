"""
advanced dataclasses 2
"""
import dataclasses

@dataclasses.dataclass
class Person:
  name: str
  age: int

@dataclasses.dataclass
class Employee(Person):
  employee_id: int
  salary: float

# Type checking
person = Person("John", 25)
employee = Employee("Alice", 30, 12345, 5000.0)

if isinstance(person, Person):
  print(person.name)  # John

if isinstance(employee, Person):
  print(employee.name)  # Alice

if isinstance(employee, Employee):
  print(employee.employee_id)  # 12345
  print(employee.salary)  # 5000.0
