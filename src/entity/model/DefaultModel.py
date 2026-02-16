import sklearn
from sklearn.ensemble import AdaBoostRegressor, RandomForestRegressor, GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor
import lightgbm
from lightgbm import LGBMRegressor
import xgboost
from xgboost import XGBRegressor
from src.entity.model.Model import Model
from src.entity.model.DefineModel import get_DNN
from typing import Dict, List, Callable

class AdaBoost(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "adaboost"    # 需要与配置项中的格式一致 -> 到时候是根据modelName找类
        self.constructor: Callable = AdaBoostRegressor

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

class RandomForest(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "randomforest"
        self.constructor: Callable = RandomForestRegressor

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

class GradientBoosting(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "gbdt"
        self.constructor: Callable = GradientBoostingRegressor

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

class MLP(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "mlp"
        self.constructor: Callable = MLPRegressor

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

class LightGBM(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "lightgbm"
        self.constructor: Callable = LGBMRegressor

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

class XGBoost(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "xgboost"
        self.constructor: Callable = XGBRegressor

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

class DNN(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "dnn"
        self.constructor: Callable = get_DNN

    def build(self, defaultParams: Dict[str, any]):
        """构造模型"""
        self.modelObj = self.constructor(**defaultParams)

