import os, json, json5
import tqdm
import numpy as np
import pandas as pd
import dolphindb as ddb
from src.callback import selectFactor
from src.entity.model.Model import Model
from src.entity.source.DataSource import DataSource
from src.entity.selector.Selector import Selector
from typing import Dict, List, Callable
from src.utils.utils import getClassFromString
np.random.seed(42)

class FactorModel(Model):
    def __init__(self, session: ddb.session,
                 modelDict: Dict[str, str],
                 factorDict:  Dict[str, str],
                 labelDict: Dict[str, str],
                 timeDict: Dict[str, List[str]],
                 factorSelectFunc: Callable):
        super().__init__()
        self.session = session
        self.modelDict = modelDict
        self.modelNameDict = {name: Dict["modelName"] for name, Dict in self.modelDict.items()}
        self.modelClassDict = {self.modelNameDict[name]: getClassFromString(Dict["modelObj"])
                               for name, Dict in self.modelDict.items()}
        self.modelObjDict = {}
        self.factorDict = factorDict
        self.labelDict = labelDict
        self.dataSource = DataSource(session, factorSelectFunc=factorSelectFunc)
        self.dataSource.init(factorDict, labelDict)
        self.selector = Selector()
        self.selector.setTimeRule(timeDict)
        self.labelName: str = ""
        self.timeDict: Dict[pd.Timestamp, List[pd.Timestamp]] = {}

    def run(self, startDate: pd.Timestamp, labelName: str, namePrefix: str = "test_"):
        """
        训练PipeLine
        :param startDate: 开始训练日期
        :param labelName: 标签名称
        :param namePrefix: 最终保存的因子名称 = namePrefix+modelName
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
            fileStrName = pd.Timestamp(currentDate).strftime("%Y%m%d")
            predStartDate, predEndDate = self.selector.getNextPeriod()
            currentStartDate, currentEndDate = self.selector.forward()
            print("currentDate", currentDate)
            print("currentStartDate", currentStartDate, "currentEndDate", currentEndDate)
            # 触发回调函数 -> 获取当前日期下的因子列表
            factorList = self.dataSource.getFactorList(labelName=labelName, currentDate=currentDate)
            # 获取训练数据 + 预测数据
            trainData = self.dataSource.getData(startDate=currentStartDate, endDate=currentEndDate,
                                                symbolList=None, labelList=[self.labelName], factorList=factorList)
            trainData = trainData[~trainData[self.labelName].isna()].reset_index(drop=True)
            if trainData.empty:
                continue
            trainY = trainData[self.labelName]
            filterFactorList = [i for i in factorList if i in trainData.columns]
            trainX = trainData[filterFactorList]
            predData = self.dataSource.getFactor(startDate=predStartDate, endDate=predEndDate,
                                                 symbolList=None, factorList=filterFactorList)
            if predData.empty:
                continue
            print("predStartDate", predData[self.dataSource.dataDateCol].min(),
                  "predEndDate", predData[self.dataSource.dataDateCol].max())
            predX = predData[filterFactorList]

            # 遍历当前的所有模型配置(dnn_v0, dnn_v1,...)
            for name in self.modelDict.keys():
                modelName = self.modelNameDict[name]    # dnn
                modelClass = self.modelClassDict[modelName]
                # 构造模型 + 初始化模型
                model = modelClass()
                model.fromDict(Dict=self.modelDict[name])
                if modelName in ["dnn","resnet"]:
                    model.build(inputDim=len(filterFactorList),
                                defaultParams=self.modelDict[name]["default_params"])
                else:
                    model.build(defaultParams=self.modelDict[name]["default_params"])
                self.modelObjDict[name] = model

                # 训练模型
                model.train(trainX, trainY)

                # 选择模型
                if modelName in ["dnn","resnet"]:
                    model.select(X=trainX, y=trainY, inputDim=len(filterFactorList))
                else:
                    model.select(X=trainX, y=trainY, inputDim=None)

                # 保存模型
                model.save(fileName=fileStrName, targetFormat="bin")

                # 进行预测
                comp = model.pred(predX)

                # 合成因子写入数据库
                factorName: str = namePrefix+name
                self.dataSource.append(index=predData[[self.dataSource.dataSymbolCol, self.dataSource.dataDateCol]],
                                       value=comp, factorName=factorName)
            self.modelObjDict = {}

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
                    factorSelectFunc=selectFactor)
    F.run(startDate="2023.09.14", labelName="ret10D", namePrefix="")