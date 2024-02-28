#!/usr/bin/env python
# coding: utf-8

# In[10]:


from pyspark.sql import SparkSession


# In[11]:


spark=SparkSession.builder.appName('Dataframe').getOrCreate()


# In[12]:


spark


# In[13]:


##Read the dataset
df_pyspark=spark.read.option('header','true').csv("test 2.csv",inferSchema=True)


# In[14]:


##check the schema
df_pyspark.printSchema()


# In[15]:


df_pyspark=spark.read.csv('test 2.csv',header=True,inferSchema=True)
df_pyspark.show()


# In[16]:


df_pyspark.head(3)


# In[17]:


df_pyspark.show()


# In[18]:


df_pyspark.select(['Name','Experience'])


# In[19]:


df_pyspark['Name']


# In[20]:


df_pyspark.dtypes


# In[21]:


df_pyspark.describe().show()


# In[22]:


##Adding Columns in dataframe
df_pyspark=df_pyspark.withColumn('Experience After two years', df_pyspark['Experience']+2)


# In[23]:


df_pyspark.show()


# In[24]:


# drop the columns
df_pyspark=df_pyspark.drop('Experience After two years')


# In[25]:


df_pyspark.show()


# In[26]:


#rename the column
df_pyspark.withColumnRenamed('Name','new name').show()


# In[30]:


#sorting as per Age
df_pyspark=df_pyspark.sort("Age")


# In[31]:


df_pyspark.show()


# In[ ]:




