import os, json, json5
import tqdm
import pandas as pd
import dolphindb as ddb
from src.callback import callBack
from src.entity.model.Model import Model
from src.entity.source.DataSource import DataSource
from src.entity.selector.Selector import Selector
from typing import Dict, List, Callable
from src.utils.utils import getClassFromString

class FactorModel(Model):
    def __init__(self, session: ddb.session,
                 modelDict: Dict[str, str],
                 factorDict:  Dict[str, str],
                 labelDict: Dict[str, str],
                 timeDict: Dict[str, List[str]],
                 callBackFunc: Callable):
        super().__init__()
        self.session = session
        self.modelDict = modelDict
        self.modelClassDict = {getClassFromString(Dict["modelObj"]) for _, Dict in self.modelDict.items()}
        self.modelObjDict = {}
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
            # 1.根据时间规则滚动向前 -> 获取数据
            predStartDate, predEndDate = self.selector.getNextPeriod()
            currentStartDate, currentEndDate = self.selector.forward()
            # 触发回调函数 -> 获取当前日期下的因子列表
            factorList = self.dataSource.getFactorList(labelName=labelName, currentDate=currentDate)
            # 获取训练数据 + 预测数据
            trainData = self.dataSource.getData(startDate=currentStartDate, endDate=currentEndDate,
                                                symbolList=None, labelList=[self.labelName], factorList=factorList)
            trainY = trainData[self.labelName]
            filterFactorList = [i for i in factorList if i in trainData.columns]
            trainX = trainData[filterFactorList]
            predData = self.dataSource.getFactor(startDate=predStartDate, endDate=predEndDate,
                                                 symbolList=None, factorList=filterFactorList)
            predX = testData[filterFactorList]

            # # 训练模型
            for name, modelClass in self.modelClassDict.items():    # 遍历当前的所有模型配置
                model = modelClass()
                model.build()

            # 保存模型

            # 进行预测

            # 合成因子写入数据库
            print("currentDate", currentDate, "currentStartDate", currentStartDate, "currentEndDate", currentEndDate)
            print("train", trainData["tradeDate"].min(), trainData["tradeDate"].max())
            print("test", testData["tradeDate"].min(), testData["tradeDate"].max())

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
    F = FactorModel(session,
                    modelDict=modelDict,
                    factorDict=factorDict,
                    labelDict=labelDict,
                    timeDict=timeDict,
                    callBackFunc=callBack)
    print(F.modelClassDict)
    # F.run(startDate="2021.01.01", labelName="ret10D")