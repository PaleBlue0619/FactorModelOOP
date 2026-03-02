import os, json, json5
import tqdm
import numpy as np
import pandas as pd
import dolphindb as ddb
from src.callback import selectFactor, combineFactor
from src.entity.source.DataSource import DataSource
from src.entity.selector.Selector import Selector
from typing import Dict, List, Callable
from src.utils.utils import getClassFromString
np.random.seed(42)

# 有些没必要进机器学习&深度学习的因子合成方法
# 1.过去一段时间IC特别好的因子 -> 按IC加权/等权

class FactorLightModel:
    def __init__(self, session: ddb.session,
                 factorDict: Dict[str, str],
                 labelDict: Dict[str, str],
                 timeDict: Dict[str, List[str]],
                 factorSelectFunc: Callable,
                 factorCombineFunc: Callable    # 简单因子的合成回调函数
                 ):
        self.session = session
        self.factorDict = factorDict
        self.labelDict = labelDict
        self.dataSource = DataSource(session, factorSelectFunc=factorSelectFunc, factorCombineFunc=factorCombineFunc)
        self.dataSource.init(factorDict, labelDict)
        self.selector = Selector()
        self.selector.setTimeRule(timeDict)
        self.labelName: str = ""
        self.timeDict: Dict[pd.Timestamp, List[pd.Timestamp]] = {}

    def run(self, startDate: pd.Timestamp, labelName: str, factorNames: List[str]):
        """
        合成Pipeline -> 支持同时合成多个因子
        :param startDate: 开始日期
        :param labelName: 使用到的标签
        :param factorNames: 返回的因子名称列表
        """
        startDate = pd.Timestamp(startDate)
        self.labelName = labelName  # 目前只支持一个标签
        # 按照时间进行训练
        if startDate >= max(self.selector.timeDict.keys()):
            return # 说明当前时间规则设计的不合理 -> 开始训练日期>所有设置
        self.selector.timeDict = {i: j for i, j in self.selector.timeDict.items() if i >= startDate}
        self.timeDict = self.selector.timeDict.copy()

        for currentDate in tqdm.tqdm(self.timeDict.keys(), desc="combining..."):
            # 1.根据时间规则滚动向前 -> 获取数据
            # 触发回调函数 -> 获取当前日期下的因子列表
            factorList = self.dataSource.getFactorList(labelName=labelName,
                                                       currentDate=currentDate)
            combineDF = self.dataSource.combineFactor(labelName=labelName, currentDate=currentDate, factorList=factorList,
                                                      factorNames=factorNames)
            self.dataSource.appendDF(data=combineDF)

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r".\cons\time.json5", "r", encoding="utf-8") as f:
        timeDict = json5.load(f)
    with open(r".\cons\source.json5", "r", encoding="utf-8") as f:
        sourceDict = json5.load(f)
    factorDict = sourceDict["factor"]
    labelDict = sourceDict["label"]
    F = FactorLightModel(session,
                         timeDict=timeDict,
                         factorDict=factorDict,
                         labelDict=labelDict,
                         factorSelectFunc=selectFactor,
                         factorCombineFunc=combineFactor)
    F.run(startDate="2021.01.01", labelName="ret5D", factorNames=["ret5D_ICComp"])

