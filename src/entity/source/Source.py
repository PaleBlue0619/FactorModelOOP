import numpy as np
import pandas as pd
import dolphindb as ddb

class Source:
    def __init__(self, session: ddb.session):
        self.session: ddb.session = session
        self.X: pd.DataFrame = pd.DataFrame()
        self.y: pd.DataFrame = pd.DataFrame()
        self.dateCol: str = ""
        self.symbolCol: str = ""
        self.factorDB: str = ""
        self.factorTB: str = ""
        self.labelDB: str = ""
        self.labelTB: str = ""

    def init(self, Dict: Dict[str, str]):
        self.dateCol = Dict["dateCol"]
        self.symbolCol = Dict["symbolCol"]
        self.factorDB = Dict["factorDB"]
        self.factorTB = Dict["factorTB"]
        self.labelDB = Dict["labelDB"]
        self.labelTB = Dict["labelTB"]

    def get_feature(self, startDate: pd.Timestamp, endDate: pd.Timestamp, symbolList: List[str])\
            -> pd.DataFrame:
        data = self.session.run(f"""
            select 
        """)
        return data

    def get_label(self, startDate: pd.Timestamp, endDate: pd.Timestamp, symbolList: List[str])\
            -> pd.DataFrame:
        data = self.session.run(f"""
        """)
        return data
