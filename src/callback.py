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
        callBackPeriod = 40;
        icThreshold = 0.9;
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
        icStats = select avg(rankIC) as icMean, std(rankIC) as icStd from icDF group by factor;
        
        /* 筛选因子 */
        exec factor from icStats where abs(icMean)>quantile(abs(icMean), icThreshold);
    """)
    return factorList

def combineFactor(self: DataSource, labelName: str, currentDate: pd.Timestamp, factorList: List[str], factorNames: List[str]) -> pd.DataFrame:
    """
    :param self: 数据源
    :param labelName: 所选标签
    :param currentDate: 当前日期
    :param factorList: 输入因子列表
    :param factorNames: 输出合成因子列表
    :return:
    """
    currentDate = pd.Timestamp(currentDate).strftime("%Y.%m.%d")
    self.session.upload({"factorList": factorList})
    self.session.upload({"factorNames": factorNames})
    # # 合成因子数据 -> 等权合成
    # combineData = self.session,run(f"""
    #     callBackPeriod = 20;
    #     endDate = {currentDate};
    #     startDate = temporalAdd(endDate, -1*callBackPeriod, "CFFEX");
    #     factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}")
    #                 where ({self.factorDateCol} between startDate and endDate) and {self.factorIndicatorCol} in factorList;
    #                 pivot by {self.factorSymbolCol}, {self.factorDateCol}, {self.factorIndicatorCol};
    #     filterList = columnNames(factorDF)[2:]
    #     /* 等权合成 */
    #     factorName = string(factorList[0]);
    #     <select {self.factorSymbolCol}, {self.factorDateCol}, factorName as {self.factorIndicatorCol},
    #         rowAvg(_$$filterList) as factorName>.eval(); // 元编程
    # """)

    # 合成因子数据 -> 根据IC加权合成
    # 标的列 日期列 因子值 标签值 IC值
    combineData = self.session.run(f"""
        callBackPeriod = 20;
        labelName = "{labelName}"
        endDate = {currentDate};
        startDate = temporalAdd(endDate, -1*callBackPeriod, "CFFEX");
        factorName = string(factorNames[0]);
        factorDF = select {self.factorSymbolCol} as {self.dataSymbolCol}, 
                          {self.factorDateCol} as {self.dataDateCol},
                          {self.factorIndicatorCol} as factor, 
                          {self.factorValueCol} as factorVal 
                    from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    where ({self.factorDateCol} between startDate and endDate) and {self.factorIndicatorCol} in factorList;
        labelDF = select {self.labelSymbolCol} as {self.dataSymbolCol},
                        {self.labelDateCol} as {self.dataDateCol},
                        // {self.labelIndicatorCol} as label,
                        {self.labelValueCol} as labelVal
                    from loadTable("{self.labelDBName}", "{self.labelTBName}")
                   where {self.labelIndicatorCol}==labelName and 
                   startDate<={self.dataDateCol} and {self.labelDateCol}>=startDate and {self.labelDateCol}<endDate
        factorDF = select * from lj(factorDF, labelDF, ["{self.dataSymbolCol}","{self.dataDateCol}"]) 
                    where not isNull(labelVal);
        update factorDF set factorIC = spearmanr(factorVal, labelVal) context by factor, {self.dataDateCol};
        update factorDF set factorVal = iif(factorIC<0, -factorVal, factorVal);
        select factorName as {self.factorIndicatorCol}, 
                factorIC**factorVal as factorName from factorDF group by {self.factorSymbolCol}, {self.dataDateCol};
    """)
    return combineData