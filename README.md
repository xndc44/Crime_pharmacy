**Predicting Taxi Gratuities in New York City**

**Overview**

Chicago consistently ranks among the U.S. cities with the highest crime rates. To help law enforcement and city planners allocate resources more effectively, it is crucial not only to identify existing high-crime areas but also to predict areas where future crimes may occur. Since traditional crime prediction models often lack in contextual sensitivity, this project takes a novel unconventional approach by using pharmacy locations to enhance crime prediction.

**Data Understanding**

Data was sourced from data.cityofchicago.org, comprising over 8 million crime records and features including: date, primary type, arrest, district, domestic, location description, and location. Additionally, pharmacy locations across the city were extracted and mapped by ZIP code.

Key Visual Insights:

Crime Frequency:
As seen in the bar chart, theft, battery, and criminal damage are the most frequent offenses. Theft alone accounts for nearly 200,000 incidents, though arrest rates vary significantly by crime type.

<img src="https://github.com/user-attachments/assets/482ad86d-b8c0-4070-aed6-85eb03e20bcb" alt="Most Common Types of Crime" width="600">

Crime Trends Over Time:
In recent years, the number of these top crimes sharply rose around 2020, possibly due to socio-economic disruptions during the COVID-19 pandemic.

<img src="https://github.com/user-attachments/assets/36751fbf-4676-4b36-99bc-0a8b729b229b" alt="Most Common Types of Crime" width="600">

Crime Geography:
The heatmap reveals that crimes are heavily concentrated in South and West Chicago ZIP codes, particularly where the count ranges from 200,000 to over 360,000 incidents.

<img src="https://github.com/user-attachments/assets/eaebb799-3fcd-40a2-bc86-4c73fae1db8b" alt="Most Common Types of Crime" width="600">

Pharmacy Density:
Pharmacies seem to be more densely located in North and Central Chicago, often aligning inversely with the highest crime zones—suggesting a disparity in community infrastructure.

<img src="https://github.com/user-attachments/assets/cc328301-7a0b-4af0-b33f-356c2c4b6ada" alt="Most Common Types of Crime" width="600">


**Modeling and Evaluation**

After removing null and malformed rows, the datasets were transformed by:
1. Geospatial preprocessing of latitude and longitude columns;
2. Vectorization and standardization using Spark’s MLlib tools;
3. Clustering via K-Means to detect spatial patterns;
4. Distance analysis using Euclidean metrics between crime clusters and pharmacy locations.

Clusters with high crime density were spatially compared with pharmacy distribution. While pharmacies do not directly cause or prevent crime, their presence often correlates with better infrastructure, foot traffic, and surveillance—factors that can influence criminal activity.

**Conclusion**

Hotspot Identification:
The most crime-prone zones are between longitude -87.8 to -87.5 and latitude 41.65 to 42.05. This corresponds to areas in South and West Chicago, where crime rates are historically high and pharmacy density is low.

Predicted Future Crime Areas:
Based on clustering and the inverse relationship with pharmacy locations, ZIP codes in mid-southern Chicago that currently have rising crime trends but low infrastructure density are the most likely regions for future crimes. Specific ZIP codes falling in this prediction zone include parts of Englewood, West Englewood, Washington Park, and Greater Grand Crossing.

Limitations:
Due to computational constraints, only a subset of the full dataset was modeled. Running the full dataset in a more scalable environment (e.g., full Spark cluster or cloud-based solution like AWS EMR) would yield more robust results.
