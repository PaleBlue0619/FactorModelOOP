import numpy as np
import pandas as pd
import dolphindb as ddb
from src.entity.source.Source import Source
from src.entity.source.LabelSource import LabelSource
from typing import List, Dict, Callable

class DataSource(LabelSource):
    def __init__(self, session: ddb.session,
                 factorSelectFunc: Callable, # 回调函数 -> 入参: currentDate -> 自动返回所需的因子
                 factorCombineFunc: Callable = None # 回调函数: FactorModel_light 专属
                 ):
        super().__init__(session)
        self.factorSelectFunc: Callable = factorSelectFunc
        self.factorCombineFunc: Callable = factorCombineFunc

    def getFactorList(self, labelName: str, currentDate: pd.Timestamp) -> List[str]:
        """
        获取当前日期+当前标签下选择的因子列表
        :param labelName: 目标标签
        :param currentDate: 当前日期
        :return:
        """
        return self.factorSelectFunc(self, labelName, currentDate)

    def combineFactor(self, labelName: str, currentDate: pd.Timestamp, factorList: List[str], factorNames: List[str]) -> pd.DataFrame:
        """
        合成因子
        :param labelName: 所选标签
        :param currentDate: 当前日期
        :param factorList: 所选因子列表
        :param factorNames: 输出因子列表
        :return:
        """
        return self.factorCombineFunc(self, labelName, currentDate, factorList, factorNames)

    def append(self, factorName: str, index: pd.DataFrame, value: List[float]) -> None:
        """
        向因子库写入因子
        最终格式: symbol tradeDate factor value
        :param factorName: 因子名称
        :param index: symbol tradeDate
        :param data: value
        :return:
        """
        index = index.copy()
        index["factor"] = factorName
        index["value"] = value
        self.factorAppender.append(index)

    def appendDF(self, data: pd.DataFrame):
        self.factorAppender.append(data)

    def getData(self, startDate: pd.Timestamp = None,
                endDate: pd.Timestamp = None,
                symbolList: List[str] = None,
                labelList: List[str] = None,
                factorList: List[str] = None
                ) -> pd.DataFrame:
        """获取完整的数据集 -> startDate & endDate
        通过LabelSource进行获取
        """
        # 目前只支持一个标签 -> TODO: 支持多个标签
        [realStartDate, realEndDate] = self.getDateListFromLabel(startDate, endDate, labelList[0])
        realStartDate = pd.Timestamp(realStartDate).strftime("%Y.%m.%d")
        realEndDate = pd.Timestamp(realEndDate).strftime("%Y.%m.%d")
        if symbolList is None:
            symbolList = []
        self.session.upload({"symbolList": symbolList})
        if labelList is None:
            labelList = []
        self.session.upload({"labelList": labelList})
        if factorList is None:
            factorList = []
        self.session.upload({"factorList": factorList})
        data = self.session.run(f"""
            startDate = {realStartDate}
            endDate = {realEndDate}            
            /* 标签内存表 */
            if (size(symbolList)==0 and size(labelList)==0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where {self.labelDateCol} between startDate and endDate and ({self.labelCondition})
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else if(size(symbolList)>0 and size(labelList)==0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and {self.labelSymbolCol} in symbolList and ({self.labelCondition})
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else if(size(symbolList)==0 and size(labelList)>0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and {self.labelIndicatorCol} in labelList and ({self.labelCondition})
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else{{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and ({self.labelSymbolCol} in symbolList) and ({self.labelIndicatorCol} in labelList) and ({self.labelCondition}) 
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}

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

            /* 进行合并 */
            matchingCols = ["{self.dataSymbolCol}", "{self.dataDateCol}"]
            labelDF = select * from lj(labelDF, factorDF, matchingCols);

            /* 清理内存并返回结果 */
            undef(`factorDF)
            labelDF;
        """.replace("and ()", ""))
        return data