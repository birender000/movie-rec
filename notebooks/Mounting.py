# Databricks notebook source
adlsAccountName = "sa2mrlandingzone"
adlsContainerName = "validated"
adlsFolderName = "Data"

# we are mounting to this place.
mountPoint = "/mnt/Files/Validated"

# we need to create scope in url below after workspace id.
# cluster url tillworkspace id  then add #secrets/createScope

#application (client) id

# now we are giving command for use our scope and get id from there.
applicationId = dbutils.secrets.get(scope="scope-mr",key="client-id-mr")

#client secret key
authenticationKey = dbutils.secrets.get(scope="scope-mr",key="secret-mr")

#tenanat id
tenantId=dbutils.secrets.get(scope="scope-mr",key="tenant-id-mr")

#--boiler code.

endpoint = "https://login.microsoftonline.com/"+tenantId+"/oauth2/token"
source="abfss://"+adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+adlsFolderName




#conneting using service principal secrets and OAuth
configs = {  
"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": applicationId,
"fs.azure.account.oauth2.client.secret": authenticationKey,
"fs.azure.account.oauth2.client.endpoint": endpoint
}

#mounting ADLS storage to DBMS
#mounting only if directory is not already mounted.

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source=source, mount_point=mountPoint, extra_configs=configs)


# COMMAND ----------

print(applicationId)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/Files/Validated

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------


