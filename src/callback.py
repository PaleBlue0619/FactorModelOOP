import pandas as pd
from src.entity.source.DataSource import DataSource
from typing import List

def callBack(self: DataSource, labelName: str, currentDate: pd.Timestamp) -> List[str]:
    """
    回调的方式获取每期选取的因子 -> 这里的currentDate为收盘后的时间!!!
    """
    currentDate = pd.Timestamp(currentDate).strftime("%Y.%m.%d")
    factorList: List[str] = self.session.run(f"""
        /* 配置参数 */
        callBackPeriod = 20;
        currentDate = {currentDate};
        labelName = "{labelName}";
        factorDB = "{self.factorDBName}";
        factorTB = "{self.factorTBName}";
        labelDB = "{self.labelDBName}";
        labelTB = "{self.labelTBName}";
        
        /* 交易日历 */
        startDate = temporalAdd(currentDate,-1*callBackPeriod, "CFFEX" ); // callBackPeriod之前的交易日
        endDate = currentDate
        
        /* 取数 */
        labelDF = select {self.labelSymbolCol} as {self.symbolCol}, 
                         {self.labelDateCol} as {self.dateCol},
                         {self.labelValueCol} as {self.valueCol}
                  from loadTable(labelDB, labelTB)
                  where {self.labelIndicatorCol}==labelName and startDate<={self.dateCol} and {self.labelDateCol}>=startDate and {self.labelDateCol}<endDate
        
    """)
    return factorList
