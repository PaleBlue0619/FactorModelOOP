import pandas as pd
from typing import Dict, List

class TimeSelector: # Walk Forward Analysis 的时间模块
    """解决基于当前时间选择何种历史时间的问题"""
    def __init__(self):
        self.currentDate: pd.Timestamp = None
        self.totalStartDate: pd.Timestamp = None    # 本次训练任务的总开始时间
        self.totalEndDate: pd.Timestamp = None  # 本次训练任务的总结束时间
        self.factorDict: Dict[pd.Timestamp, List[str]] = None    # 因子列表

    def forward(self, dataSql: str, nearMatch: bool = True) -> None:
        """
        向前滚动策略 -> 每次回调会更新哪些属性
        如果该天选择的特征为空 -> 往前取最靠近的一天
        exp.
        sql: select
        """
        # 取出当前时间对应的因子



