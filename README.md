#### - See DatabaseSchema.md for creating database schema
#### - See SQLQueries.md for database querying
 ---
## Data cleaning 
Infer_datetime_format had been removed in the latest pandas and I decided to stick with the new version instead of reverting as there was some new functianlities that I was making use of. This resulted in me running the to_datetime method multiple times with different formats. Null values and duplicates were always the first ones to be checked. I saved the unclean data to a new file and the clean data to another new file to compare the two datasets. This was to see whether my expected cleaning had taken place and look for room for improvements.The terminal was not printing the pandas dataframe as extensively and this was an amazing way to see what I was doing. Working with tabula for pdf reading took a couple tries but we got there at the end and boto3 was pretty smooth due to having past experience with it.

## Key things to consider
1. More than one person cannot have the same number - duplicates recognised via subset - kept the first instance and deleted others
1. Make sure that all of the data formats are the same
1. Any duplicated data is taken care of and any null values are removed
1. Infer where possible

## Creating the database schema
There was a lot of converting data types involved to make the columns were the right data types before developing the star schema. There were cases were duplicate columns especially with the index columns were present, so I had to apply drop column. UUID data types on the unique identifiers (user_uuid and dater_uuid). Then I created the primary and foreign keys to finalise the star-based databse schema.

## Querying the data
This section was the best bit as it allowed me to query through the data that I had cleaned and put together. It was satisfying to get the same response as the expected result and working with ctes was a bit of a challenge. It was a great way to solidify my sql querying skills and definitely learnt a lot along the way. I was fairly confident with joins and subqueries beforehand and this task reconsolidated that for me.

By Thilakshan Balasubramaniam