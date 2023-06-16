import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import xgboost as xgb
from sklearn.metrics import recall_score, accuracy_score, precision_score, f1_score, confusion_matrix, roc_auc_score
from sklearn.metrics import classification_report
from sklearn.utils.class_weight import compute_class_weight
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import  RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression

def balance_set(data):

    n_true = sum(data.fraud == 1)

    data_0_ind = data.loc[data.fraud == 0].index.tolist()
    data_1_ind = data.loc[data.fraud == 1].index.tolist()

    false_index = np.random.choice(data_0_ind, size=n_true, replace=False)
    data_0 = data.loc[false_index]
    data_1 = data.loc[data_1_ind]

    output = pd.concat([data_0, data_1])
    return output.sample(frac=1)




data_train = pd.read_csv("data_train.csv", index_col=0)
scale = sum(data_train.fraud == 0) / (1.0*sum(data_train.fraud == 1))
data_val = pd.read_csv("data_validation.csv", index_col=0)
drop_cols = ["fraud"]


def print_scores(labels, model_eval, model, data=data_val.drop(columns=drop_cols)):


    print(f"Accuracy: {accuracy_score(labels, model_eval)}")
    print(f"Recall: {recall_score(labels, model_eval)}")
    print(f"Precision: {precision_score(labels, model_eval)}")
    print(f"F1: {f1_score(labels, model_eval)}")
    print(f"AUROC:" +str(roc_auc_score(labels, model.predict_proba(data)[:, 1])))


    confusion = confusion_matrix(labels, model_eval)

    plt.figure(figsize=(10, 7))
    sns.heatmap(confusion, annot=True)
    plt.show()


    '''    fig = plt.figure(figsize = (14, 9))
    ax = fig.add_subplot(111)

    colours = plt.cm.Set1(np.linspace(0, 1,20))

    ax = xgb.plot_importance(xgb_base, height = 1, color = colours, grid = False, \
                             show_values = False, importance_type = 'cover', ax = ax);
    for axis in ['top','bottom','left','right']:
        ax.spines[axis].set_linewidth(2)

    ax.set_xlabel('importance score', size = 16);
    ax.set_ylabel('features', size = 16);
    ax.set_yticklabels(ax.get_yticklabels(), size = 12);
    ax.set_title('Ordering of features by importance to the model learnt', size = 20)

    plt.show()
    '''




#data_train = balance_set(data_train)
'''
xgb_base = xgb.XGBClassifier(learning_rate=0.000001,
                             scale_pos_weight=scale,
                             n_estimators=400,
                             max_depth=8,
                             n_jobs=4

                            )
'''

'''xgb_base = xgb.XGBClassifier(learning_rate=0.001,
                             max_depth=20,
                             subsample=0.8,
                             n_estimators=4*250,
                             scale_pos_weight=scale,
                             n_jobs=4,
                             colsample_bytree = 0.6)
xgb_base.fit(data_train.drop(columns=drop_cols), data_train.fraud)
base_pred = xgb_base.predict(data_val.drop(columns=drop_cols))'''

'''random_forest_base = RandomForestClassifier()
random_forest_base.fit(balance_set(data_train).drop(columns=drop_cols), balance_set(data_train).fraud)
random_forest_base_pred = random_forest_base.predict(data_val.drop(columns=drop_cols))


tree_base = DecisionTreeClassifier()
tree_base.fit(balance_set(data_train).drop(columns=drop_cols), balance_set(data_train).fraud)
tree_base_pred = tree_base.predict(data_val.drop(columns=drop_cols))

logit = LogisticRegression()
logit.fit(balance_set(data_train).drop(columns=drop_cols), balance_set(data_train).fraud)
logit_pred = logit.predict(data_val.drop(columns=drop_cols))
'''

#print(f"AUROC:" +str(roc_auc_score(data_train.fraud, xgb_base.predict_proba(data_train.drop(columns=drop_cols))[:, 1])))
#print_scores(data_val.fraud, base_pred, xgb_base, data=data_val.drop(columns=drop_cols))
'''

print_scores(data_val.fraud, random_forest_base_pred, random_forest_base)
print_scores(data_val.fraud, tree_base_pred, tree_base)
print_scores(data_val.fraud, logit_pred, logit)
'''

'''#cross val search
xgb_grid = xgb.XGBClassifier()

param_grid = {"n_estimators": [100, 250, 500],
              "max_depth": range(8, 18, 2),
              "learning_rate": [ 0.001, 0.01, 0.1],
            "scale_pos_weight": [scale],
              "n_jobs":[-1],
              "colsample_bytree": [ 0.3, 0.6, 0.9],
              "subsample": [0.5, 0.8, 1]
              }

grid_model = GridSearchCV(estimator=xgb_grid, param_grid=param_grid, cv=5,  verbose=3, n_jobs=-1,
                          scoring=["recall", "accuracy"], refit="recall")

grid_model.fit(data_train.drop(columns=drop_cols), data_train.fraud)
print(grid_model.best_params_)
xgb_best = grid_model.best_estimator_
y_pred = xgb_best.predict(data_val.drop(columns=drop_cols))

print(classification_report(data_val.fraud, y_pred))
print_scores(data_val.fraud, y_pred, xgb_best)'''