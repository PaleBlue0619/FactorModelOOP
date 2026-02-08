import pandas as pd
from typing import Dict, List

class Time: # Walk Forward Analysis 的时间模块
    def __init__(self):
        self.periodList: List[int] = []
        self.currentPeriod: int = 0

    def init(self, startDate: pd.Timestamp, Dict) -> None:
        """
        :param startDate: 回测开始日期
        :param Dict:
        """

    def forward(self) -> None:
        """
        向前滚动策略
        """
