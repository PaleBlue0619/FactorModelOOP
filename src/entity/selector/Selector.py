import pandas as pd
import numpy as np
import dolphindb as ddb
from typing import Dict, List
# from src.entity.selector import TimeSelector, DataSelector

class Selector: # (TimeSelector, DataSelector):
    """训练数据: 因子数据(X) & 标签(Y)选择器
    选择器应同时选择X(因子)与y(标签), 通过select函数获取完整用于训练的面板数据
    时间维度的选择器 -> 字典
    factor: 时间选择器-因子
    {
      "date1": [startDate, endDate],
      "date2": [startDate, endDate]
    }
    label: 时间选择器-标签
    {
      "date1": [startDate, endDate],
      "date2": [startDate, endDate]
    }
    空间维度的选择器 -> sqlStr
    exp.
    data.cond select * from x where $dateCol between _$startDate and _$endDate
    """
    def __init__(self):
        self.data: pd.DataFrame = None          # 基于固定SQL规则的数据清洗 -> 空间维度的Selector
        self.currentDate: pd.Timestamp = None   # 基于已有时间选择过去时间 -> 时间维度的Selector
        self.session: ddb.session = None
        self.params: Dict[str, any] = {}        # 选择器超参数
        self.symbolCol: str = ""
        self.dateCol: str = ""
        self.totalSql: str = ""

    def setCurrentDate(self, date: pd.Timestamp):
        self.currentDate = pd.Timestamp(date)

    def setSession(self, session: ddb.session):
        self.session = session

    def setTimeRule(self, Dict: Dict[str, List[str]]):
        """设置时间规则"""
        timeDict = {}   # 转化为Dict[pd.Timestamp, List[pd.Timestamp]]
        for key, value in Dict.items():   # [startDate, endDate]
            timeDict[pd.Timestamp(key)] = [pd.Timestamp(value[0]), pd.Timestamp(value[1])]

    def select(self) -> pd.DataFrame:
        """根据currentDate进行选择"""
        data = self.session.run(f"""
            
        """)
        return data


