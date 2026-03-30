# ML-Blood Sugar Level Example

You can find the source for the example
[here](https://github.com/cls-python/cosy-luigi/tree/main/examples/ml_blood_sugar_level/):

Here we used CLS-Luigi to create a ML Pipeline to predict the blood
sugar level of some patients. The Steps are very easy to understand. We
start by loading the dataset from Scikit-Learn, then we split it into 2
subsets for training and testing.

The first variation point is the scaling method. We introduce 2 concrete
implementation, namely `RobustScaler` & `MinMaxScaler`. After scaling we
have our second variation point which is the regression model. Here we
have also 2 concrete implementation, namely `LinearRegression` &
`LassoLars`.

Lastly we evaluate each regression model by predicting the testing
target and calculating the root mean squared error.

# Requirements

The example contains a
[requirements.txt](https://github.com/cls-python/cls-luigi/tree/main/examples/ml_blood_sugar_level/requirements.txt)
file. To experiment with the example, you can set up your environment by
executing the following command:

``` bash
# cd into the ml blood sugar level example folder
pip install -r requirements.txt
```

# Static Visualization

![image](images/static.png)

# Dynamic Visualization

![image](images/dynamic.png)
