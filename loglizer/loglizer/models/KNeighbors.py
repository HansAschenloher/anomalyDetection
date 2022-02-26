"""
The implementation of the LocalOutlierFactor for anomaly detection.

Authors:
    Johann Aschenloher

Reference:
    LOF: Identifying Density-Based Local Outliers, by Markus M. Breunig, Hans-Peter Kriegel, Raymond T. Ng, Jörg Sander.
"""



import numpy as np
from sklearn.neighbors import NearestNeighbors  as NN
from ..utils import metrics

class KNeighbors(NN):
    def __init__(self, n_neighbors=10, radius=1.0, algorithm='auto', leaf_size=30, metric='minkowski', p=2, contamination=0.03):
        """
        Auguments
        ---------
        n_neighbors: int, default=20
        radius: float, default=1.0
        algorithm: {‘auto’, ‘ball_tree’, ‘kd_tree’, ‘brute’}, default=’auto’
        leaf_size:int, default=30
        metric: str or callable, default=’minkowski’
        p: int, default=2
        metric_params: dict, default=None. Additional keyword arguments for the metric function.
        contamination: ‘auto’ or float, default=’auto’
        novelty: bool, default=False. True if you want to use LocalOutlierFactor for novelty detection.
            In this case be aware that you should only use predict, decision_function and score_samples
            on new unseen data and not on the training set.
        n_jobs: int, default=None. The number of parallel jobs to run for neighbors search.
            None means 1 and -1 means all Processors

        Reference
        ---------
            For more information, please visit https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.LocalOutlierFactor.html
	    """

        self.n_neighbors = n_neighbors
        self.contamination=contamination
        super(KNeighbors, self).__init__(n_neighbors=n_neighbors, radius=radius, algorithm=algorithm, leaf_size=leaf_size, metric=metric, p=p)

    def fit(self, X, y=None):
        """
        Auguments
        ---------
            X: ndarray, the event count matrix of shape num_instances-by-num_events
        """

        print('====== Model summary ======')
        super(KNeighbors, self).fit(X)
        distancesAndPoints = self.kneighbors(X, self.n_neighbors + 1, return_distance=True)
        distances = list(map(sum, distancesAndPoints[0]))
        distances.sort()
        self.threshold = distances[int(len(distances)*(1-self.contamination))]

    def predict(self, X):
        """

        Arguments
        ---------
            X: the input event count matrix

        Returns
        -------
            y_pred: ndarray, the predicted label vector of shape (num_instances,)
        """

        distancesAndPoints = self.kneighbors(X, self.n_neighbors + 1, return_distance=True)
        distances = list(map(sum, distancesAndPoints[0]))
        y_pred = np.where(distances < self.threshold, 0, 1)
        return y_pred

    def evaluate(self, X, y_true):
        print('====== Evaluation summary ======')
        y_pred = self.predict(X)
        precision, recall, f1 = metrics(y_pred, y_true)
        print('Precision: {:.3f}, recall: {:.3f}, F1-measure: {:.3f}\n'.format(precision, recall, f1))
        return precision, recall, f1

