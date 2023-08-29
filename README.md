>###[versions]
>
>#####spark=3.4.1;
>#####hadoop=3.2.4; 
>#####scala=2.12.18

# ScalaChallenge

Small project written in scala, using Apache Spark that solves some tasks that target two data sources:

- mock_data_users.csv  
- mock_data_movies.csv

In the resolution off all tasks this information was assumed:

- Today's date was considered to be "2020/01/01".
- The dollar is vallued at 0.90 of the euro.


###Task 1 

>Generate a single CSV file with the top 10 movies by score. In case of a draw order alphabetical by title. Use ";" as the CSV delimiter. 

###Task 2 

>Generate a single CSV file with the best 3 movies for each genre. In case of a draw order alphabetical by title. Use ";" as the CSV delimiter. 

###Task 3 

>Generate a single CSV file with the list of users who are older than 18 years old. Use ";" as the CSV delimiter. 

###Task 4 

>Write a single Parquet file with the same data contained in the “mock_data_users.csv” file. 
> 
>But split the value of the money and the currency to different columns. 

Example:

|4|John Doe| Geraga45@gmail.com| Male|$44.24| 4.11.1996 |
|---|---|---|---|---|---|

Becomes:

|4|John Doe| Geraga45@gmail.com| Male|USD|44.24| 4.11.1996 |
|---|---|---|---|---|---|---|

>Use the following columns:

Name|Type 
---|---
id|Integer
name|String 
email|String
gender|String 
currency|String 
money|Double 
birthdate|Timestamp


###Task 5 

>Generate a single CSV with the list of users that can afford to buy all movies.
> 
>Use ";" as the CSV delimiter. 

###Task 6 

>Generate a single Parquet file with the list of users and the movies each user can watch in a new array column called “available_movies”.
> 
>Horror movies can only be watched by adults.



