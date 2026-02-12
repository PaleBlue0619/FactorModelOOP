import os
import pickle
import numpy as np
import sklearn
from sklearn.base import BaseEstimator
from sklearn.model_selection._search import GridSearchCV
import torch.nn as nn
from typing import Dict, Callable

class Model:
    def __init__(self):
        self.modelName: str = ""
        self.defaultParams: Dict[str, any] = {}
        self.gridParams: Dict[str, any] = {}
        self.modelPath: str = ""
        if not self.modelPath:
            os.mkdir(self.modelPath)
        self.gridObj: BaseEstimator = None
        self.constructor: Callable = None   # modelObj构造函数
        self.modelObj = None

    def fromDict(self, modelName: str, Dict: Dict[str, any]):
        """从字典进行初始化"""
        self.modelName: str = modelName
        self.defaultParams: Dict[str, any] = Dict["default_params"]
        self.gridParams: Dict[str, any] = Dict["grid_params"]

    def train(self, X, y):
        """训练模型"""
        self.gridObj: BaseEstimator = None
        self.gridObj.fit(X, y)

    def reTrain(self, X, y):
        """增量训练模型"""
        self.gridObj.fit(X, y)

    def pred(self):
        """应用模型"""
        best_params, best_score = self.gridObj.best_params_, self.gridObj.best_score_
        self.modelObj = self.constructor(**best_params)

    def load(self, fileName: str):
        """加载模型"""
        full_path = os.path.join(self.modelPath, fileName)
        with open(full_path, 'rb') as f:
            self.modelObj = pickle.load(f)

    def save(self, fileName: str):
        """保存模型"""
        full_path = os.path.join(self.modelPath, fileName)
        with open(full_path, 'rb') as f:
            return pickle.dump(f.read())