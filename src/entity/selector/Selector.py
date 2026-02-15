import pandas as pd
import numpy as np
import dolphindb as ddb
from typing import Dict, List

class Selector:
    """训练数据: 因子数据(X) & 标签(Y)选择器
    选择器应同时选择X(因子)与y(标签), 通过select函数获取完整用于训练的面板数据
    时间维度的选择器 -> 字典
    factor: 时间选择器-因子
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
        self.params: Dict[str, any] = {}        # 选择器超参数
        self.timeDict: Dict[pd.Timestamp, List[pd.Timestamp]] = {}
        self.currentIdx: int = 0

    def setTimeRule(self, Dict: Dict[str, List[str]]):
        """设置时间规则"""
        timeDict = {}   # 转化为Dict[pd.Timestamp, List[pd.Timestamp]]
        for key, value in Dict.items():   # [startDate, endDate]
            timeDict[pd.Timestamp(key)] = [pd.Timestamp(value[0]), pd.Timestamp(value[1])]
        self.timeDict = timeDict

    def forward(self) -> [pd.Timestamp, pd.Timestamp]:
        if self.currentIdx >= len(self.timeDict):
            return None # 说明已经训练到头了
        self.currentDate = list(self.timeDict.keys())[self.currentIdx]
        self.currentIdx += 1
        return self.timeDict[self.currentDate]

