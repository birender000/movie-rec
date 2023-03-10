# movie-reccomendation end to end project.

Service used:
==============

1. Azure data factory (Integration)
2. Databricks (transformation)
3. Key vault (secrets)
4. blob storage (raw data storage)


Architecture
================
                                               
1. blob [raw data] 
2. get metadata and check for all files received
3. schema validation and filename validation
4. copy data with wrong filename and schema to blob[reject] and send failure mail.                 
5. copy data with correct filename and schema to blob[validate]  
6. mount notebook and do requrired transformation and model training. 
7. link databricks with access token key 
8. send mail with recommended movies.
                                                                                                                                                                                                        


Leanings
===========================

1. File name validation
2. Schema validation
3. App registration
4. Creation of secrets keys in key vault
5. Databricks scope creation and link to key vault
6. Data cleaning
7. Data transformation and analysis
8. Model training and testing
9. Data bricks accesss in ADF using acess token key
10. Mail integration.


--Birender Singh
