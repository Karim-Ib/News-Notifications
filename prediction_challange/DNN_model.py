import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import tensorflow as tf

'''from tensorflow.python import keras
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.optimizers import RMSprop
from keras.metrics import AUC'''

def balance_set(data, upsample=False):

    if not upsample:
        n_true = sum(data["fraud"] == 1)

        data_0_ind = data.loc[data["fraud"] == 0].index.tolist()
        data_1_ind = data.loc[data["fraud"] == 1].index.tolist()

        false_index = np.random.choice(data_0_ind, size=n_true, replace=False)
        data_0 = data.loc[false_index]
        data_1 = data.loc[data_1_ind]

        output = pd.concat([data_0, data_1])
        return output.sample(frac=1)
    if upsample:
        n_false = sum(data["fraud"] == 0 )

        data_0_ind = data.loc[data["fraud"] == 0].index.tolist()
        data_1_ind = data.loc[data["fraud"] == 1].index.tolist()

        true_index = np.random.choice(data_1_ind, size=n_false, replace=True)
        data_0 = data.loc[data_0_ind]
        data_1 = data.loc[true_index]

        output = pd.concat([data_0, data_1])
        return output.sample(frac=1)



data_train = pd.read_csv("data_train_norm.csv", index_col=0)
scale = sum(data_train.fraud == 0) / (1.0*sum(data_train.fraud == 1))
data_val = pd.read_csv("data_val_norm.csv", index_col=0)
drop_cols = ["fraud"]


#data = balance_set(data_train, True)
data = data_train

weight_for_0 = (1 / len(data_train.fraud[data_train.fraud==0])) * (len(data_train.fraud) / 2.0)
weight_for_1 = (1 / len(data_train.fraud[data_train.fraud==1])) * (len(data_train.fraud) / 2.0)

class_weight = {0: weight_for_0, 1: weight_for_1}
initial_bias = np.log([ len(data_train.fraud[data_train.fraud==1])/ len(data_train.fraud[data_train.fraud==0])])



model = tf.keras.Sequential()
model.add(tf.keras.layers.Dense(4*512, activation = 'relu', input_shape = (data_train.shape[1]-1,)))
model.add(tf.keras.layers.Dense(4*256, activation = 'relu'))
model.add(tf.keras.layers.Dense(2*128, activation = 'relu'))
model.add(tf.keras.layers.Dense(2*64, activation = 'relu'))
model.add(tf.keras.layers.Dense(2*32, activation = 'relu'))
model.add(tf.keras.layers.Dense(32, activation = 'relu'))
model.add(tf.keras.layers.Dense(32, activation = 'relu'))
model.add(tf.keras.layers.Dense(4, activation = 'relu'))
model.add(tf.keras.layers.Dense(2, activation = 'relu'))
#model.add(tf.keras.layers.Dense(1, activation = 'sigmoid', bias_initializer=tf.keras.initializers.Constant(initial_bias)
                               # ))
model.add(tf.keras.layers.Dense(1, activation = 'sigmoid'))
print(model.summary())


model.compile(optimizer = tf.keras.optimizers.SGD(learning_rate=0.01), loss = 'binary_crossentropy', metrics = [tf.keras.metrics.AUC(curve = 'PR'), "acc"])
history = model.fit(data.drop(columns=drop_cols), data.fraud,
                    epochs = 25, batch_size = 512, validation_data = (data_val.drop(columns=drop_cols), data_val.fraud),
                    class_weight=class_weight)

train_auc = history.history['auc']
val_auc = history.history['val_auc']
epochs = range(1, len(train_auc) + 1)

plt.plot(epochs, train_auc, linestyle="--", label = 'Training AUPRC')
plt.plot(epochs, val_auc, linestyle="--", color="orange", label='Validation AUPRC')
plt.title('Training and validation AUPRC')
plt.xlabel('Epochs')
plt.ylabel('AUPRC')
plt.ylim([0.0, 1.05])
plt.legend(loc='lower right')
plt.show()


acc = history.history['auc']
val_acc = history.history['val_auc']
loss = history.history['loss']
val_loss = history.history['val_loss']
epochs = range(1, len(acc) + 1)

plt.plot(epochs, acc, linestyle="--",  label='Training acc')
plt.plot(epochs, val_acc, linestyle="--", color="orange", label='Validation acc')
plt.title('Training and validation accuracy')
plt.legend()

plt.figure()

plt.plot(epochs, loss, linestyle="--", label='Training loss')
plt.plot(epochs, val_loss, linestyle="--", color="orange", label='Validation loss')
plt.title('Training and validation loss')
plt.legend()

plt.show()