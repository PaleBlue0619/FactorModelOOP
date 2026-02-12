import json5
import dolphindb as ddb # 交易日历
import pandas as pd
from typing import List, Dict

def generateTime(startDate: str, endDate: str, freq: str) -> Dict[str, List[str]]: