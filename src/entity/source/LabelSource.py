import numpy as np
import pandas as pd
import dolphindb as ddb
from src.entity.source.Source import Source

class LabelSource(Source):
    def __init__(self, session: ddb.session):
        super().__init__(session)
        self.labelDateCol1: str = ""
        self.labelDBName: str = ""
        self.labelTBName: str = ""
        self.labelDateCol: str = ""
        self.labelDateCol1: str = ""
        self.labelSymbolCol: str = ""
        self.labelIndicatorCol: str = ""
        self.labelValueCol: str = ""

    def getDateListFromLabel(self, startDate: pd.Timestamp, endDate: pd.Timestamp, labelName: str) -> List[pd.Timestamp]:
        """
        rule: startDate < tradeDate + minDate < endDate
        :return: 返回 tradeDate 的左右边界
        """
        startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        timeDF = self.session.run(f"""
        startDate = {startDate} 
        endDate = {endDate}
        select min(tradeDate) as min_date, 
                max(tradeDate) as max_date 
                from loadTable("{self.dbName}", "{self.tbName}")
                where startDate<={self.dateCol} and {self.labelDateCol}>=startDate and {self.labelDateCol}<endDate and label == "{labelName}"
        """)
        startDate = pd.Timestamp(timeDF["min_date"].iloc[0])
        endDate = pd.Timestamp(timeDF["max_date"].iloc[0])
        return [startDate, endDate]