# movie-reccomendation end to end project.

Service used:
==============

1. Azure data factory (Integration)
2. Databricks (transformation)
3. Key vault (secrets)
4. blob storage (raw data storage)


Architecture
================

                                                                                                                                                                                                          |---------------------| ====> copy data with wrong filename ====> send failure mail.                     
blob  ====> get metadata and =====>|schema validation and|           and schema to blob[reject]
[raw data]  check for              |filename validation  |         
            all files received     |---------------------| ====> copy data with correct ===> mount notebook and  ====> link databricks ===> send mail with 
                                                                  filename and schema        do requrired  with         access token key     recommended movie
                                                                  to blob[validate]           transformation                                                                                                                             and model training


Different use cases cover.
===========================

File name validation
Schema validation
data cleaning
data transformation
model training and testing
mail integration.

    
--Birender Singh
