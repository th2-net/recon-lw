from typing import Dict, Any, Callable, List, Union, Optional

Message = Dict[str, Any]

InterpretationFunctionType = Callable[[Message, Any, dict], List[dict]]
KeyFunctionType = Callable[[Message], Union[Optional[List[str]], Optional[str]]]