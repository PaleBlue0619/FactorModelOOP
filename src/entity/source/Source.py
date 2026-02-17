import numpy as np
import pandas as pd
import dolphindb as ddb
from typing import List, Dict

class Source:
    def __init__(self, session: ddb.session):
        self.session: ddb.session = session
        self.factorDateCol: str = ""
        self.labelDateCol: str = ""
        self.labelDateCol1: str = ""  # minDate
        self.factorSymbolCol: str = ""
        self.labelSymbolCol: str = ""
        self.factorDBName: str = ""
        self.labelDBName: str = ""
        self.factorTBName: str = ""
        self.labelTBName: str = ""
        self.factorIndicatorCol: str = ""
        self.labelIndicatorCol: str = ""
        self.factorSymbolCol: str = ""
        self.labelSymbolCol: str = ""
        self.factorValueCol: str = ""
        self.labelValueCol: str = ""
        self.factorCondition: str = ""
        self.labelCondition: str = ""
        self.dataDateCol: str = "tradeDate"
        self.dataSymbolCol: str = "symbol"
        self.factorAppender: ddb.TableAppender = None

    def init(self, factorDict: Dict[str, str], labelDict: Dict[str, str]):
        self.factorDBName = factorDict["dbName"]
        self.factorTBName = factorDict["tbName"]
        self.factorDateCol = factorDict["dateCol"]
        self.factorSymbolCol = factorDict["symbolCol"]
        self.factorIndicatorCol = factorDict["indicatorCol"]
        self.factorValueCol = factorDict["valueCol"]
        self.factorCondition = factorDict["condition"]
        self.labelDBName = labelDict["dbName"]
        self.labelTBName = labelDict["tbName"]
        self.labelDateCol = labelDict["dateCol"]
        self.labelDateCol1 = labelDict["labelDateCol"]
        self.labelSymbolCol = labelDict["symbolCol"]
        self.labelIndicatorCol = labelDict["indicatorCol"]
        self.labelValueCol = labelDict["valueCol"]
        self.labelCondition = labelDict["condition"]
        self.factorAppender = ddb.TableAppender(dbPath=self.factorDBName,
                                                tableName=self.factorTBName,
                                                ddbSession=self.session)

    def getFactor(self, startDate: pd.Timestamp = None,
                  endDate: pd.Timestamp = None,
                  symbolList: List[str] = None,
                  factorList: List[str] = None
                  ) -> pd.DataFrame:
        """只获取特征, 不获取标签"""
        if symbolList is None:
            symbolList = []
        self.session.upload({"symbolList": symbolList})
        if factorList is None:
            factorList = []
        self.session.upload({"factorList": factorList})
        startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        data = self.session.run(f"""
            startDate = {startDate}
            endDate = {endDate}            
            /* 因子内存表 */
            if (size(symbolList)==0 and size(factorList)==0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where {self.factorDateCol} between startDate and endDate and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else if(size(symbolList)>0 and size(factorList)==0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and {self.factorSymbolCol} in symbolList and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else if(size(symbolList)==0 and size(factorList)>0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and {self.factorIndicatorCol} in factorList and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else{{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and ({self.factorSymbolCol} in symbolList) and ({self.factorIndicatorCol} in factorList) and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            /* 返回结果 */
            factorDF
        """.replace("and ()",""))
        return data
