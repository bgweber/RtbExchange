# Databricks notebook source
import tensorflow as tf
from tensorflow.io import FixedLenFeature
import numpy as np
import pandas as pd
import sklearn
import tensorflow_recommenders as tfrs
import keras

# use databricks arguments for parameters
def get_argument(parameter):
    dbutils.widgets.text(parameter, '')
    return dbutils.widgets.get(parameter)

# script parameters, parse from JSON input to the job task
train_path_base = get_argument("train_path_base")
test_path_base = get_argument("test_path_base")
model_output_path = get_argument("model_output_path")
metrics_output_path = get_argument("metrics_output_path")
print("train_path_base: " + train_path_base)
print("test_path_base: " + test_path_base)
print("model_output_path: " + model_output_path)
print("metrics_output_path: " + metrics_output_path)

# additional script parameters
num_numeric_features = 6
num_category_features = 2
batch_size = 10
num_epochs = 10
initial_bias = np.log(0.05)
learning_rate=1e-2

# list of category features, ordered based on the reshape step, the vocab size, and embedding layer size
category_features = [
    ("country_code", 6, 3),
    ("timezone", 10, 3)
]

# list of TF record files. we are only using a single file for train and test
train_paths = [train_path_base + "part-r-00000"]
test_paths = [test_path_base + "part-r-00000"]

def getRecords(paths):
    features = {
        'numeric': FixedLenFeature([num_numeric_features], tf.float32),
        'categorical': FixedLenFeature([num_category_features], tf.int64),
        'is_retrained': FixedLenFeature([1], tf.int64)
    }

    @tf.function
    def _parse_example(x):
        f = tf.io.parse_example(x, features)
        return f, f.pop("is_retrained")

    dataset = tf.data.TFRecordDataset(train_paths)
    dataset = dataset.batch(batch_size)
    dataset = dataset.map(_parse_example)
    return dataset

training_data = getRecords(train_paths)
test_data = getRecords(test_paths)


# model definition
input_layers = []
dense_inputs = []

# numeric inputs
numeric_input = keras.Input(shape=(num_numeric_features), name = "numeric", dtype='float32')
input_layers.append(numeric_input)
dense_inputs.append(numeric_input)

# category inputs
categories_input = keras.Input(shape=(num_category_features), name = "categorical", dtype='int64')
input_layers.append(categories_input)

for i, (feature, vocab_size, embed_size) in enumerate(category_features):
    category_input = categories_input[:,(i):(i+1)]
    embedding = keras.layers.Flatten()(keras.layers.Embedding(vocab_size, embed_size, name = feature + "_embedding")(category_input))
    dense_inputs.append(embedding)

input_values = keras.layers.Concatenate(name = "dense_inputs")(dense_inputs)

# cross layers
cross_layer = tfrs.layers.dcn.Cross(projection_dim=None, kernel_initializer="glorot_uniform", preactivation="swish")
crossed_ouput = cross_layer(input_values, input_values)

cross_layer = tfrs.layers.dcn.Cross(projection_dim=None, kernel_initializer="glorot_uniform", preactivation="swish")
crossed_ouput = cross_layer(input_values, crossed_ouput)

# output layer
bias = tf.keras.initializers.Constant([initial_bias])  # set an initial biad to help with over-fitting, use 5% as the starting point for expected conversion
sigmoid_output = keras.layers.Dense(1, activation="sigmoid", bias_initializer=bias)(crossed_ouput)

model = keras.Model(inputs=input_layers, outputs = [ sigmoid_output ], name = "retained")

# model fitting
metrics=[tf.keras.metrics.AUC(), tf.keras.metrics.AUC(curve="PR")]
model.compile(optimizer=keras.optimizers.Adam(learning_rate=learning_rate),loss=tf.keras.losses.BinaryCrossentropy(), metrics=metrics)
history = model.fit(x = training_data, epochs = num_epochs, validation_data = test_data, verbose=0)

# save the model
model.save(model_output_path) 

# calculate model metrics
y_true = np.concatenate([list(y.numpy().flat) for x, y in test_data], axis=0)
y_pred = model.predict(test_data)
y_pred_fixed = list(y_pred.flat)
y_true_fixed = list(y_true.flat)

pred_conv = np.mean(y_pred_fixed)
act_conv = np.mean(y_true_fixed)
roc = sklearn.metrics.roc_auc_score(y_true_fixed,y_pred_fixed)
pr = sklearn.metrics.average_precision_score(y_true_fixed,y_pred_fixed)
brier = sklearn.metrics.brier_score_loss(y_true_fixed,y_pred_fixed)

# save model metrics
metrics_df = spark.createDataFrame(pd.DataFrame.from_dict({
   "Predicted_Conv": float(pred_conv),
   "Actual_Conv": float(act_conv),
   "ROC_AUC": float(roc),
   "PR_AUC": float(pr),
   "Brier": brier
}, orient='index').T)

metrics_df.write.mode("overwrite").parquet(metrics_output_path)

