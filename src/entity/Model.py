import os
import pickle
import numpy as np
import sklearn
from sklearn.base import BaseEstimator
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
        self.modelObj: BaseEstimator = None

    def fromDict(self, modelName: str, Dict: Dict[str, any]):
        """从字典进行初始化"""
        self.modelName: str = modelName
        self.defaultParams: Dict[str, any] = Dict["default_params"]
        self.gridParams: Dict[str, any] = Dict["grid_params"]

    def train(self):
        """训练模型"""
        pass

    def pred(self):
        """应用模型"""
        pass

    def load(self) -> :
        """加载模型"""
        pass

    def save(self):
        """保存模型"""
        pass