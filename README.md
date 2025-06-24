**Predicting Taxi Gratuities in New York City**

**Overview**

The city of Chicago has one of the highest crime rates in the US. This is a cause for concern for current and future residents, as most would prefer to live in an area with little or no crime. For this reason, it is necessary to identify areas of high crime and predict where the next crime could potentially occur, so the crime can be prevented. Since traditional crime prediction methods are ineffective, a non-traditional method must be used. Hence, this project involves the use of pharmaceutical locations to predict the location of the next crime.

**Data Understanding**

The NYC Taxi and Limousine Commission data came from data.cityofchicago.org. The data consisted of approximately 8164329 rows and 7 features. The features included information on date, primary type, arrest, district, domestic, location description, and location. The bar chart below shows the breakdown of the most common types of crimes and their arrest rate.

<img src="https://github.com/user-attachments/assets/482ad86d-b8c0-4070-aed6-85eb03e20bcb" alt="Most Common Types of Crime" width="600">

The following graph shows the change in the top 3 most common crimes over the years.

<img src="https://github.com/user-attachments/assets/36751fbf-4676-4b36-99bc-0a8b729b229b" alt="Most Common Types of Crime" width="600">

The crime distribution by zip code.

<img src="https://github.com/user-attachments/assets/eaebb799-3fcd-40a2-bc86-4c73fae1db8b" alt="Most Common Types of Crime" width="600">

The pharmacy distribution by zip code.

<img src="https://github.com/user-attachments/assets/cc328301-7a0b-4af0-b33f-356c2c4b6ada" alt="Most Common Types of Crime" width="600">

**Modeling and Evaluation**

To use the data properly, first, multiple null rows were dropped. Next, the columns containing the latitude and longitude values were isolated for each dataset: latitude and longitude for the crime dataset and New Georeferenced Column for the pharmacy dataset. A floating-point conversion was then attempted for each data point. If the conversion failed, it was replaced with a zero and filtered out of the dataset. Next, the x and y coordinates of the data were zipped together and used to create a data frame using Apache Spark. The next step involved using k-means clustering, a machine learning algorithm used to group data points based on their similarity. The goal is to create a clustered heatmap based on the dataset to identify areas of high crime and use this to aid in the prediction. To achieve this, the Euclidean distance was calculated between the pharmaceutical locations and crime locations, and the spark_min function was used to identify the minimum distance between the two locations. The crime dataset is then combined with the minimum distance and pharmaceutical data frames. Spark’s vector assemble was used to combine the columns into one, and then it was further preprocessed by Spark’s standard scaler. The data was passed into a model that used k-means clustering to identify patterns within the dataset. To test the model, the code was run in the Databricks environment.

**Conclusion**

In conclusion, it has been identified that areas from x = -87.8 to -87.5, y=41.65 to 42.05 are the areas where crime occurs, and are therefore the areas to have future crimes occur. Therefore, the police force should be focused on these areas. Furthermore, future residents should avoid these areas. The data drew inconclusive results on the relationship between crime and pharmaceutical locations. A limitation this project faced had to do with the environment being used, as the majority of the data to be used could not be included in the dataset. An improvement to this experiment would be to use an environment capable of handling the full crime dataset, and perhaps this experiment could be redone with another parameter, such as restaurant locations.
