import numpy as np
import pandas as pd
import dolphindb as ddb
from src.entity.source.Source import Source
from src.entity.source.LabelSource import LabelSource

class DataSource(LabelSource):
    def __init__(self, session: ddb.session):
        super().__init__(session)
        self.factorDateCol: str = ""
        self.labelDateCol: str = ""
        self.labelDateCol1: str = ""    # minDate
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
        self.dataDateCol: str = "tradeDate"
        self.dataSymbolCol: str = "symbol"

    def init(self, factorDict: Dict[str, str], labelDict: Dict[str, str]):
        self.factorDBName = factorDict["dbName"]
        self.factorTBName = factorDict["tbName"]
        self.factorDateCol = factorDict["dateCol"]
        self.factorSymbolCol = factorDict["symbolCol"]
        self.factorIndicatorCol = factorDict["indicatorCol"]
        self.factorValueCol = factorDict["valueCol"]
        self.labelDBName = labelDict["dbName"]
        self.labelTBName = labelDict["tbName"]
        self.labelDateCol = labelDict["dateCol"]
        self.labelDateCol1 = labelDict["labelDateCol"]
        self.labelSymbolCol = labelDict["symbolCol"]
        self.labelIndicatorCol = labelDict["indicatorCol"]
        self.labelValueCol = labelDict["valueCol"]

    def getData(self, startDate: pd.Timestamp = None,
                endDate: pd.Timestamp = None,
                symbolList: List[str] = None,
                labelList: List[str] = None,
                factorList: List[str] = None) -> pd.DataFrame:
        """获取完整的数据集 -> startDate & endDate
        通过LabelSource进行获取
        """
        [realStartDate, realEndDate] = self.getDateListFromLabel(startDate, endDate, "label")
        realStartDate = pd.Timestamp(realStartDate).strftime("%Y.%m.%d")
        realEndDate = pd.Timestamp(realEndDate).strftime("%Y.%m.%d")
        if not symbolList:
            symbolList = []
        self.session.upload({"symbolList": symbolList})
        if not labelList:
            labelList = []
        self.session.upload({"labelList": labelList})
        if not factorList:
            factorList = []
        self.session.upload({"factorList": factorList})
        data = self.session.run(f"""
            startDate = {realStartDate}
            endDate = {realEndDate}
            symbolList = {self.symbolList}
            factorList = {self.factorList}
            labelList = {self.labelList}
            
            /* 标签内存表 */
            if (size(symbolList)==0 and size(labelList)==0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where {self.labelDateCol} between startDate and endDate
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else if(size(symbolList)>0 and size(labelList)==0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and {self.labelSymbolCol} in symbolList
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else if(size(symbolList)==0 and size(labelList)>0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and {self.labelIndicatorCol} in labelList
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else{{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and ({self.labelSymbolCol} in symbolList) and ({self.labelIndicatorCol} in labelList) 
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            
            /* 因子内存表 */
            if (size(symbolList)==0 and size(factorList)==0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where {self.factorDateCol} between startDate and endDate
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else if(size(symbolList)>0 and size(factorList)==0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and {self.factorSymbolCol} in symbolList
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else if(size(symbolList)==0 and size(factorList)>0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and {self.factorIndicatorCol} in factorList
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else{{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and ({self.factorSymbolCol} in symbolList) and ({self.factorIndicatorCol} in factorList) 
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            
            /* 进行合并 */
            matchingCols = ["{self.dataSymbolCol}", "{self.dataDateCol}"]
            labelDF = select * from lj(labelDF, factorDF, matchingCols);
            
            /* 清理内存并返回结果 */
            undef(`factorDF)
            labelDF;
        """)
        return data