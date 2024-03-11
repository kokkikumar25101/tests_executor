import importlib
import pkgutil

def get_subclasses(parent_class, package_name):
  package = importlib.import_module(package_name)
  subclasses = []
  for _, name, _ in pkgutil.iter_modules(package.__path__):
    module = importlib.import_module(f"{package_name}.{name}")
    for name, obj in module.__dict__.items():
      if isinstance(obj, type) and issubclass(obj, parent_class) and obj != parent_class:
        subclasses.append(obj)
  return subclasses