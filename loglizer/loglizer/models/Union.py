"""
Implementation to build the Union of Models.

Authors:
Johann Aschenloher

"""
from ..utils import metrics

class UnionModel():
    def __init__(self,model_list):
        self.models = model_list

    def fit(self, X, y=None):
        for m in self.models:
            m.fit(X)

    def predict(self, X):
        y_pred = np.zeros(len(X))
        for m in self.models:
            #y_pred = map(operator.add, m.predict(X))
            y_pred = [ a or b for (a,b) in zip(y_pred,m.predict(X)) ]
        return list(y_pred)

    def evaluate(self, X, y_true):
        print('====== Evaluation summary ======')
        y_pred = self.predict(X)
        precision, recall, f1 = metrics(y_pred, y_true)
        print('Precision: {:.3f}, recall: {:.3f}, F1-measure: {:.3f}\n'.format(precision, recall, f1))
        return precision, recall, f1

