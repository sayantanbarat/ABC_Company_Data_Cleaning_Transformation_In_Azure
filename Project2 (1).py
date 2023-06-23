# Databricks notebook source
#get the file name from the adf
#fileName = dbutils.widgets.get('fileName')
fileName = 'Product.csv'
fileNameWithoutExt = fileName.split('.')[0]
print(fileNameWithoutExt)

# COMMAND ----------

import pyspark.sql.functions as F
#from datetime import datetme as dt

#Just change all the values here based on the resource name you have created in your environemnt and workspace.

sqlDbName = 'proj2dbsb'
dbUserName = 'proj2dbsb'
passwordKey = 'dbkey'
stgAccountSASTokenKey = 'sastoken'
landingFileName =fileName #'Product'  #dbutils.widgets.get('Product')
databricksScopeName ='myscope'
dbServer = 'proj2dbsb'
dbServerPortNumber ='1433'
storageContainer ='input'
storageAccount='proj2storagesb'
landingMountPoint ='/mnt'


# COMMAND ----------

if not any(mount.mountPoint == landingMountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format(storageContainer, storageAccount), mount_point= landingMountPoint, extra_configs ={'fs.azure.sas.{}.{}.blob.core.windows.net'.format(storageContainer,storageAccount):dbutils.secrets.get(scope = databricksScopeName, key= stgAccountSASTokenKey)})
    print('Mounted the storage account successfully')
else:
    print('Storage account already mounted')

# COMMAND ----------

#connect to Azure SQL DB
dbPassword = dbutils.secrets.get(scope = databricksScopeName, key= passwordKey)
serverurl = 'jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(dbServer, dbServerPortNumber,sqlDbName,dbUserName)
connectionProperties = {
    'password':dbPassword,
    'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}
df = spark.read.jdbc(url = serverurl, table = 'dbo.FileDetailsFormat', properties= connectionProperties)
display(df)


# COMMAND ----------

df1 = spark.read.csv('/mnt/landing/'+fileName, inferSchema=True, header=True)
display(df1)

# Rule
errorFlag=False
errorMessage = ''
totalcount = df1.count()
print(totalcount)
distinctCount = df1.distinct().count()
print(distinctCount)
if distinctCount !=totalcount:
    errorFlag = True
    errorMessage = 'Duplication Found. Rule 1 Failed'
print(errorMessage)
    
# Rule 2
df2 = df.filter(df.FileName==fileNameWithoutExt).select('ColumnName','ColumnDateFormat' )
rows = df2.collect()
for r in rows:
    colName = r[0]
    colFormat =r[1]
    print(colName, colFormat)
    #display(df1.filter(F.to_date(colName, colFormat).isNull() ==True))
    formatCount =df1.filter(F.to_date(colName, colFormat).isNotNull() ==True).count()
    if formatCount != totalcount:
        errorFlag = True
        errorMessage = errorMessage +' DateFormate is incorrect for {} '.format(colName)
    else:
        print('All rows are good for ', colName)
print(errorMessage)

if errorFlag:
    dbutils.fs.mv('/mnt/landing/'+fileName,'/mnt/rejected/'+fileName )
    dbutils.notebook.exit('{"errorFlag": "true", "errorMessage":"'+errorMessage +'"}')
else:
    dbutils.fs.mv('/mnt/landing/'+fileName,'/mnt/staging/'+fileName )
    dbutils.notebook.exit('{"errorFlag": "false", "errorMessage":"No error"}')

# COMMAND ----------


