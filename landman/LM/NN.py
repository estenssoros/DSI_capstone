from __future__ import division
import numpy as np
import pandas as pd
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.optimizers import SGD
from keras.utils import np_utils
from LM.LM_AWS import read_from_s3
import os
from string import maketrans, punctuation
import re
from sklearn.cross_validation import train_test_split
from sklearn.metrics import precision_score, recall_score

def get_model(X, y, dropout=50, activation='tanh', node_count=64, init_func='uniform', layers=1):
    # Fit a sequential model with some given number of nodes.
    model = Sequential()

    # Fit the first hidden layer manually, becuase we have to fit it
    # with the x-shape by the node_count.
    model.add(Dense(input_dim=X.shape[1],
                    output_dim=8,
                    init=init_func,
                    activation='tanh'))
    model.add(Activation(activation))
    model.add(Dropout(dropout / 100.0))

    # We can fit any additional layers like this, provided they
    # have the same node_count (except the last one).
    for layer in xrange(layers):
        model.add(Dense(input_dim=8,
                        output_dim=8,
                        init=init_func,
                        activation='tanh'))
    model.add(Dense(input_dim=8,
                    output_dim=y.shape[1],
                    init=init_func,
                    activation='sigmoid'))

    sgd = SGD(lr=0.1, decay=1e-6, momentum=0.9, nesterov=True)
    model.compile(loss='mse', optimizer=sgd, metrics=['accuracy'])

    return model


def run_model(X_test, X_train, y_test, y_train, prob_threshold=20, layers=5, nodes=64, dropout=50):
    # Grab the model
    model = get_model(X_test, y_test, layers=layers, dropout=dropout)
    model.fit(X_train, y_train, nb_epoch=20, batch_size=16, verbose=0)

    # Get the training and test predictions from our model fit.
    train_predictions = model.predict_proba(X_train)
    test_predictions = model.predict_proba(X_test)
    # Set these to either 0 or 1 based off the probability threshold we
    # passed in (divide by 100 becuase we passed in intergers).
    train_preds = (train_predictions[:, 1]) >= prob_threshold / 100.0
    test_preds = (test_predictions[:, 1]) >= prob_threshold / 100.0

    # Calculate the precision and recall. Only output until
    precision_score_train = precision_score(y_train[:, 1], train_preds)
    precision_score_test = precision_score(y_test[:, 1], test_preds)

    recall_score_train = recall_score(y_train[:, 1], train_preds)
    recall_score_test = recall_score(y_test[:, 1], test_preds)
    print precision_score_train, precision_score_test, recall_score_train, recall_score_test
    return precision_score_train, precision_score_test, recall_score_train, recall_score_test


def read_text(fname):
    with open(fname) as f:
        text = f.read()
    text = text.lower()

    spaces = ' ' * len(punctuation)
    table = maketrans(punctuation, spaces)
    text = text.translate(table)
    text = re.sub('[^a-z 0-9]+', ' ', text)
    text = ''.join(text.split())

    return np.array([ord(char) - 96 for char in text])[:2000]


def get_data():
    df = pd.read_csv('data/results.csv')
    docs = pd.unique(df['doc'])

    files = [f.replace('.txt', '') for f in os.listdir('textdocs/') if f.endswith('.txt')]
    for doc in docs:
        if doc not in files:
            read_from_s3('textdocs/{0}.txt'.format(doc))

    X = []
    for i, r in df.iterrows():
        doc = r['doc']
        X.append(read_text('textdocs/{0}.txt'.format(doc)))
    X = np.array(X)
    return df, X


if __name__ == '__main__':
    df, X = get_data()
    y = df['township']
    y = np_utils.to_categorical(y)
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    model = get_model(X_test, y_test, layers=8, dropout=50)
    model.fit(X_train, y_train, nb_epoch=1000, verbose=1)
    x_predict = model.predict(X_test)
