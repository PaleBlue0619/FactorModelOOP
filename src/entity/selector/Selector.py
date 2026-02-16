import pandas as pd
import numpy as np
import dolphindb as ddb
from typing import Dict, List

class Selector:
    """训练数据: 特征数据(X) & 标签(Y)选择器
    选择器应同时选择X(因子)与y(标签), 通过select函数获取完整用于训练的面板数据
    时间维度的选择器 -> 字典
    factor: 特征选择器-因子
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
        self.timeDict: Dict[pd.Timestamp, List[pd.Timestamp]] = {}  # 到当前时间需要选择的历史日期
        self.factorDict: Dict[pd.Timestamp, List[str]] = {}         # 当前日期所选择的因子列表
        self.currentIdx: int = 0

    def setTimeRule(self, Dict: Dict[str, List[str]]):
        """设置时间规则"""
        timeDict = {}   # 转化为Dict[pd.Timestamp, List[pd.Timestamp]]
        for key, value in Dict.items():   # [startDate, endDate]
            timeDict[pd.Timestamp(key)] = [pd.Timestamp(value[0]), pd.Timestamp(value[1])]
        self.timeDict = timeDict

    def forward(self) -> List[pd.Timestamp]:
        """
        时间戳向前滚动
        :return: 返回当前时间戳范围
        """
        if self.currentIdx >= len(self.timeDict):
            return None # 说明已经训练到头了
        self.currentDate = list(self.timeDict.keys())[self.currentIdx]
        self.currentIdx += 1    # 下一个需要取到的时间戳
        return self.timeDict[self.currentDate]

    def getNextPeriod(self) -> List[pd.Timestamp]:
        """
        获取下一个时间戳范围
        :return: 返回下一个时间戳范围
        """
        if self.currentIdx <= len(self.timeDict.keys())-2:
            leftDate = list(self.timeDict.keys())[self.currentIdx]
            rightDate = list(self.timeDict.keys())[self.currentIdx + 1]
            return [leftDate + pd.Timedelta(1, "D"), rightDate]
        elif self.currentIdx == len(self.timeDict.keys())-1:
            leftDate = list(self.timeDict.keys())[self.currentIdx]
            rightDate = pd.Timestamp.now().date()
            return [leftDate + pd.Timedelta(1, "D"), rightDate]
        else:
            return [pd.NaT, pd.NaT]



