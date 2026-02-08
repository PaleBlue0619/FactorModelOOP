from src.entity.Model import Model

class AdaBoost(Model):
    def __init__(self):
        super().__init__()
        self.modelName: str = "AdaBoost"

    def