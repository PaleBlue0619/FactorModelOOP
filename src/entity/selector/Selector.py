import pandas as pd
import numpy as np
import dolphindb as ddb
from typing import Dict, List
from src.entity.selector import TimeSelector, DataSelector

class Selector(TimeSelector, DataSelector):
    """训练数据: 因子数据(X) & 标签(Y)选择器
    需求: 按照sql进行选择
    exp.
    data.cond select * from x where $dateCol between _$startDate and _$endDate

    """
    def __init__(self):
        self.data: pd.DataFrame = None          # 基于固定SQL规则的数据清洗 -> 空间维度的Selector
        self.currentDate: pd.Timestamp = None   # 基于已有时间选择过去时间 -> 时间维度的Selector
        self.session: ddb.Session = None
        self.params: Dict[str, any] = {}        # 选择器超参数
        self.symbolCol: str = ""
        self.dateCol: str = ""
        self.totalSql: str = ""

    def setSession(self, session: ddb.Session):
        self.session = session


