
### Toronto_crime_etl

![toronto-logo-png-transparent-e1663874591459](https://user-images.githubusercontent.com/46944817/230141486-821a51e4-6252-48d8-883d-d0727a382335.png)



### Problem:
Recently Toronto has seen a steady increase in Auto theft and other crimes. This project is to create a Cloud pipeline and analyse more about the increase in crimes. Data is available at https://open.toronto.ca/


### Datastack Used:
* Terraform
* Docker
* Prefect
* Google Cloud Storage
* Big Query
* dbt
* Looker Studio

### To Recreate the project
* See recreate.md for instructions

### Architecture:

![Drawing1](https://user-images.githubusercontent.com/46944817/230142334-7f598f19-23b5-4dab-88b3-8341350df368.jpeg)

### About data:

<img width="743" alt="image" src="https://user-images.githubusercontent.com/46944817/230143213-0bb24981-419b-4969-8456-ad9d8e9c604a.png">

### Findings:

TOP 10 Increases by Region, Year to Year Comparison:

![81444953-92FC-4732-8662-F81CF3A81553](https://user-images.githubusercontent.com/46944817/231932364-435ddfe7-4720-4e5c-b195-d9ab98c08dab.jpeg)

Dashboard:
![3860BE41-4894-4BF6-8B90-6CB0EE5D3248_1_105_c](https://user-images.githubusercontent.com/46944817/230144012-ee679e60-c580-431a-832c-f046ace91153.jpeg)


### Auto theft has increased sharply while robberies in general has decreased due to Pandemic. But is on a steady increase

### DBT Transformations:

![dbt-dag (2)](https://user-images.githubusercontent.com/46944817/231931724-87d3cbe9-b8bf-463b-a3d6-3f2697ab5733.png)

