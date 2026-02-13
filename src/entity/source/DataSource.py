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

    def getData(self, startDate: pd.Timestamp = None, endDate: pd.Timestamp = None) -> pd.DataFrame:
        """获取完整的数据集 -> startDate & endDate
        通过LabelSource进行获取
        """
        [realStartDate, realEndDate] = self.getDateListFromLabel(startDate, endDate, "label")
        realStartDate = pd.Timestamp(realStartDate).strftime("%Y.%m.%d")
        realEndDate = pd.Timestamp(realEndDate).strftime("%Y.%m.%d")
        data = self.session.run(f"""
        
        """)