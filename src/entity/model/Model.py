import os
import pickle
import numpy as np
import pandas as pd
import sklearn
from sklearn.base import BaseEstimator
from sklearn.model_selection import train_test_split
import torch.nn as nn
from typing import Dict, Callable

class Model:
    def __init__(self):
        self.modelName: str = ""
        self.earlyStop: bool = False        # 是否使用早停
        self.evalSetPercent: float = 0.1    # 划分的测试集占比
        self.seed: int = 42 # 随机种子
        self.cv: int = 5    # K-fold
        self.defaultParams: Dict[str, any] = {}
        self.gridParams: Dict[str, any] = {}
        self.modelPath: str = ""
        self.constructor: Callable = None   # modelObj构造函数
        self.modelObj = None
        self.gridObj: BaseEstimator = None

    def fromDict(self, Dict: Dict[str, any]):
        """从字典进行初始化"""
        self.modelName: str = str(Dict["modelName"])
        self.modelPath: str = str(Dict["modelPath"])
        self.cv: int = int(Dict["cv"])
        self.defaultParams: Dict[str, any] = Dict["default_params"]
        self.gridParams: Dict[str, any] = Dict["grid_params"]
        if not os.path.exists(self.modelPath):
            os.mkdir(self.modelPath)
        if "early_stopping_rounds" in self.defaultParams.keys():
            self.earlyStop = True   # 说明开启早停

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        pass

    def train(self, X: pd.DataFrame, y: pd.DataFrame):
        """训练模型"""
        if not self.earlyStop:  # 无早停机制
            self.gridObj.fit(X.values.astype(np.float32),
                             y.values.astype(np.float32))
        else:
            trainX, evalX, trainY, evalY = train_test_split(
                X.values.astype(np.float32), y.values.astype(np.float32),
                test_size=self.evalSetPercent, random_state=self.seed)
            self.gridObj.fit(trainX, trainY, eval_set=[(evalX, evalY)])

    def select(self, X: pd.DataFrame, y: pd.DataFrame, inputDim: int = None):
        """选择最优模型"""
        best_params, best_score = self.gridObj.best_params_, self.gridObj.best_score_
        if not inputDim:
            self.modelObj = self.constructor(**best_params)
        else:
            self.modelObj = self.constructor(inputDim=inputDim, **best_params)
        self.modelObj.fit(X.values.astype(np.float32), y.values.astype(np.float32)) # 选择最优参数后 -> 再把训练集放进去

    def pred(self, X: pd.DataFrame) -> np.ndarray:
        """应用模型"""
        return self.modelObj.predict(X.values.astype(np.float32))

    def load(self, fileName: str, targetFormat: str):
        """加载模型"""
        full_path = os.path.join(self.modelPath, fileName+"."+targetFormat)
        targetFormat = targetFormat.replace(".", "")
        total_format: List[str] = ["pickle", "npy", "npz", "bin"]
        if targetFormat not in total_format:
            raise ValueError("targetFormat must be one of {}".format(total_format))

        if targetFormat == 'pickle':
            with open(full_path, 'rb') as f:
                return pickle.load(f)

        elif targetFormat == 'npy':
            return np.load(full_path, allow_pickle=True)

        elif targetFormat == 'npz':
            return np.load(full_path)['model']

        elif targetFormat == 'bin':
            with open(full_path, 'rb') as f:
                return pickle.loads(f.read())

    def save(self, fileName: str, targetFormat: str):
        """保存模型"""
        save_path = os.path.join(self.modelPath, fileName+"."+targetFormat)
        targetFormat = targetFormat.replace(".", "")
        total_format: List[str] = ["pickle", "npy", "npz", "bin"]
        if targetFormat not in total_format:
            raise ValueError("targetFormat must be one of {}".format(total_format))

        if targetFormat == 'pickle':
            with open(save_path, 'wb') as f:
                pickle.dump(self.modelObj, f)

        elif targetFormat == 'npy':
            np.save(save_path, self.modelObj, allow_pickle=True)

        elif targetFormat == 'npz':
            np.savez(save_path, model=self.modelObj)

        elif targetFormat == 'bin':  # 自定义二进制格式
            with open(save_path, 'wb') as f:
                f.write(pickle.dumps(self.modelObj))