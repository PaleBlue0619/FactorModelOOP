import pandas as pd
import numpy as np
import dolphindb as ddb

class DataSelector:
    # 先执行TimeSelector -> 再执行DataSelector -> 拼接为sql -> 一起返回Data(尽可能地减少内存占用+增大磁盘计算)
    def __init__(self):
        self.currentDate: pd.Timestamp = None
        self.config: Dict[str, str] = {}
        self.session: ddb.session = None
        self.dataSql = ""

    def delete(self, dataSql: str, params: Dict[str, str]):
        """exp.
        sql: delete from _$source where _$indicator1 == 0 and _$indicator2 == 0
        params: {"indicator1": "PE", "indicator2": "ROE"}
        """
        self.dataSql = dataSql
        for key, value in params.items():
            self.dataSql = self.dataSql.replace("_$" + key, value)