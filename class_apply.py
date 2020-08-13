# Databricks notebook source
# MAGIC %md
# MAGIC ###  Defining Class

# COMMAND ----------

class multiply ():
  def __init__(self, num1, num2):
    self.num1 = num1
    self.num2 = num2

  def myfunc(self):
    return ( self.num1*self.num2)

# COMMAND ----------

p1 = multiply(10, 36)
p1.myfunc()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Defining Over Func for df

# COMMAND ----------

def mul_func(df):
  num1 = df["col1"]
  num2 = df["col2"]
  
  return multiply(num1,num2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Computation with pandas

# COMMAND ----------

import pandas as pd
d = {'col1': [2, 3], 'col2': [3, 4]}
df = pd.DataFrame(data=d)
df

# COMMAND ----------

df['y'] = df.apply(mul_func, axis = 1)

# COMMAND ----------

df

# COMMAND ----------

# it works as expected
df['y_vectorized_lambdas'] = df['y'].apply(lambda y: y.myfunc())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compution with Koalas

# COMMAND ----------

dbutils.library.installPyPI("koalas")
from databricks import koalas as ks
d = {'col1': [2, 3], 'col2': [3, 4]}
df = ks.DataFrame(data=d)
df

# COMMAND ----------

#ks.set_option("compute.ops_on_diff_frames", True)
df['y'] = df.apply(mul_func, axis = 1)

# COMMAND ----------

df

# COMMAND ----------

df['y_vectorized_lambdas'] = df['y'].apply(lambda y: y.myfunc())
