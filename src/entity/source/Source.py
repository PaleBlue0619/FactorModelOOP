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
        self.labelDBName = labelDict["dbName"]
        self.labelTBName = labelDict["tbName"]
        self.labelDateCol = labelDict["dateCol"]
        self.labelDateCol1 = labelDict["labelDateCol"]
        self.labelSymbolCol = labelDict["symbolCol"]
        self.labelIndicatorCol = labelDict["indicatorCol"]
        self.labelValueCol = labelDict["valueCol"]
        self.factorAppender = ddb.TableAppender(dbPath=self.factorDBName,
                                                tableName=self.factorTBName,
                                                ddbSession=self.session)
