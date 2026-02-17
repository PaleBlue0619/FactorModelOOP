import pandas as pd
from src.entity.source.DataSource import DataSource
from typing import List

def selectFactor(self: DataSource, labelName: str, currentDate: pd.Timestamp) -> List[str]:
    """
    回调的方式获取每期选取的因子 -> 这里的currentDate为收盘后的时间!!!
    """
    deleteFactorList = ['gbdt_v0', "randomforest_v0", "adaboost_v0",
                        'lightgbm_v0', "xgboost_v0", 'mlp_v0', 'dnn_v0']    # 防止取到自己
    currentDate = pd.Timestamp(currentDate).strftime("%Y.%m.%d")
    self.session.upload({"deleteFactorList": deleteFactorList})
    factorList: List[str] = self.session.run(f"""
        /* 配置参数 */
        callBackPeriod = 20;
        icThreshold = 0.5;
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
        labelDF = select {self.labelSymbolCol} as {self.dataSymbolCol}, 
                         {self.labelDateCol} as {self.dataDateCol},
                         {self.labelValueCol} as labelVal
                  from loadTable(labelDB, labelTB)
                  where {self.labelIndicatorCol}==labelName and 
                  startDate<={self.dataDateCol} and {self.labelDateCol}>=startDate and {self.labelDateCol}<endDate
        factorDF = select {self.factorSymbolCol} as {self.dataSymbolCol},
                          {self.factorDateCol} as {self.dataDateCol},
                          {self.factorIndicatorCol} as factor,
                          {self.factorValueCol} as factorVal
                  from loadTable(factorDB, factorTB)
                  where {self.factorDateCol} between startDate and endDate 
                    and factor not in deleteFactorList;
        factorDF = select * from lj(factorDF, labelDF, ["{self.dataSymbolCol}", "{self.dataDateCol}"]) where not isNull(labelVal);
        undef(`labelDF);
        
        /* 计算历史IC */
        icDF = select corr(factorVal, labelVal) as IC,spearmanr(factorVal, labelVal) as rankIC 
                from factorDF group by {self.dataDateCol}, factor;
        icStats = select avg(ic) as icMean, std(ic) as icStd from icDF group by factor;
        
        /* 筛选因子 */
        exec factor from icStats where abs(icMean)>quantile(abs(icMean), icThreshold);
    """)
    return factorList
