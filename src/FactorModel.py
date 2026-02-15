import os, json, json5
import tqdm
import pandas as pd
import dolphindb as ddb
from src.callback import callBack
from src.entity.source.DataSource import DataSource
from src.entity.selector.Selector import Selector
from typing import Dict, List

class FactorModel:
    def __init__(self, session: ddb.session,
                 factorDict:  Dict[str, str],
                 labelDict: Dict[str, str],
                 timeDict: Dict[str, List[str]],
                 callBackFunc: callable):
        self.session = session
        self.factorDict = factorDict
        self.labelDict = labelDict
        self.dataSource = DataSource(session, callBackFunc=callBackFunc)
        self.dataSource.init(factorDict, labelDict)
        self.selector = Selector()
        self.selector.setTimeRule(timeDict)
        self.labelName: str = ""
        self.timeDict: Dict[pd.Timestamp, List[pd.Timestamp]] = {}

    def run(self, startDate: pd.Timestamp, labelName: str, nearMatching: bool = False):
        """
        训练PipeLine
        :param startDate: 开始训练日期
        :param labelName: 标签名称
        :param nearMatching:
        :return:
        """
        startDate = pd.Timestamp(startDate)
        self.labelName = labelName  # 目前只支持一个标签
        # 按照时间进行训练
        if startDate >= max(self.selector.timeDict.keys()):
            return  # 说明当前时间规则设计的不合理 -> 开始训练日期>所有设置的时间规则
        self.selector.timeDict = {i: j for i, j in self.selector.timeDict.items() if i >= startDate} # filter
        self.timeDict = self.selector.timeDict.copy()

        for currentDate in tqdm.tqdm(self.timeDict.keys(), desc="training..."):
            currentStartDate, currentEndDate = self.selector.forward()
            factorList = self.dataSource.getFactorList(labelName=labelName, currentDate=currentDate)
            data = self.dataSource.getData(
                startDate=currentStartDate,
                endDate=currentEndDate,
                symbolList=None,
                labelList=[self.labelName],
                factorList=factorList)
            print(data)

if __name__ == "__main__":
    with open(r".\cons\time.json5", "r", encoding="utf-8") as f:
        timeDict = json5.load(f)
    with open(r".\cons\source.json5", "r", encoding="utf-8") as f:
        sourceDict = json5.load(f)
    with open(r".\cons\model.json5", "r", encoding="utf-8") as f:
        modelDict = json5.load(f)
    factorDict = sourceDict["factor"]
    labelDict = sourceDict["label"]
    session = ddb.session("localhost", 8848, "admin", "123456")
    F = FactorModel(session, factorDict=factorDict, labelDict=labelDict, timeDict=timeDict, callBackFunc=callBack)
    F.run(startDate="2020.01.01", labelName="ret5D")