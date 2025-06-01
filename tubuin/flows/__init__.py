import importlib
import pkgutil

# Dynamically load all flows in this folder
for _, name, _ in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{name}")