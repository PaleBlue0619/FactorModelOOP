import importlib
from typing import Type, Any

def getClassFromString(classPath: str) -> Type[Any]:
    """
    Dynamically import and return a class from a string path.
    """
    modulePath, className = classPath.rsplit(".", 1)
    module = importlib.import_module(modulePath)
    modelClass = getattr(module, className)
    return modelClass
