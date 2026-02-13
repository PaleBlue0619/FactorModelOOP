import numpy as np
import pandas as pd
import dolphindb as ddb

class Source:
    def __init__(self, session: ddb.session):
        self.session: ddb.session = session
        self.dbName: str = ""
        self.tbName: str = ""
        self.dateCol: str = ""
        self.symbolCol: str = ""
        self.indicatorCol: str = ""
        self.valueCol: str = ""

    def get_data(self, startDate: pd.Timestamp = None, endDate: pd.Timestamp = None,
                 symbolList: List[str] = None, indicatorList: List[str] = None
                 ) -> pd.DataFrame:
        if not startDate:
            startDate = pd.Timestamp("20200101")
        if not endDate:
            endDate = pd.Timestamp.now()
        startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        if not symbolList:
            symbolList = []
        self.session.upload({"symbolList": symbolList})
        if not indicatorList:
            indicatorList = []
        self.session.upload({"indicatorList": indicatorList})

        data = self.session.run(f"""
            startDate = {startDate}
            endDate = {endDate}
            symbolList = {self.symbolList}
            indicatorList = {self.indicatorList}
            if (size(symbolList)==0 and size(indicatorList)==0){{
                pt = select value from loadTable("{self.dbName}","{self.tbName}") 
                where {self.dateCol} between startDate and endDate
                pivot by cont, tradeDate, label
            }}
            else if(size(symbolList)>0 and size(indicatorList)==0){{
                pt = select value from loadTable("{self.dbName}","{self.tbName}") 
                where ({self.dateCol} between startDate and endDate) and {self.symbolCol} in symbolList
                pivot by cont, tradeDate, label
            }}
            else if(size(symbolList)==0 and size(indicatorList)>0){{
                pt = select value from loadTable("{self.dbName}","{self.tbName}") 
                where ({self.dateCol} between startDate and endDate) and {self.indicatorCol} in indicatorList
                pivot by cont, tradeDate, label
            }}
            else{{
                pt = select value from loadTable("{self.dbName}","{self.tbName}") 
                where ({self.dateCol} between startDate and endDate) and ({self.symbolCol} in symbolList) and ({self.indicatorCol} in indicatorList) 
                pivot by cont, tradeDate, label
            }}
            pt
        """)
        return data
