# BIG DATA
### PONTIFÍCIA UNIVERSIDADE CATÓLICA DO PARANÁ
###PROF. JEAN PAUL BARDDAL
# Big Data Processing with MapReduce

You and your team were hired to process data using MapReduce. Your company has access to a
dataset with commercial transactions between countries during the past 30 years. For each transaction,
the dataset contains the following variables:

Variable | (column) Description
---|---|
Country Country | involved in the commercial transaction
Year | Year in which the transaction took place
Commodity code | Commodity identifier
Commodity | Commodity description
Flow | Flow, e.g. Export or Import
Price | Price, in USD
Weight | Commodity weight
Unit | Unit in which the commodity is measured, e.g. Number of items
Amount | Commodity amount given in the aforementioned unit
Category | Commodity category, e.g. Live animals

The dataset has over 8 million instances (rows, or commercial transactions). The dataset is made
available in CSV format. Columns are separated by semi-colons (“;”). The image below exhibits the first 5
rows of the dataset:

country_or_area|year|comm_code|commodity|flow|trade_usd|weight_kg|quantity_name|quantity|category
---|---|---|---|---|---|---|---|---|---|
Afghanistan|2016|010410|Sheep, live|Export|6088|2339|Number of items|51|01_live_animals
Brazil|2016|010210|Bovine animals, live pure-bred breeding|Export|374888|59132|Number of items|129|01_live_animals

![](https://i.imgur.com/yxzez4A.png)


Given the aforementioned context, you are in charge of developing a set of solutions that allow
the company to answer the following demands:

1. [x] The number of transactions involving Brazil;
2. [x] The number of transactions per year;
3. [x] The most commercialized commodity (summing the quantities) in 2016, per flow type.
4. [x] The average of commodity values per year;
5. [x] The average price of commodities per unit type, year, and category in the export flow in
    Brazil;
6. [x] The commodity with the highest price per unit type and year;
7. [x] The number of transactions per flow type and year.

Given your knowledge and skills in Java and MapReduce, for each item above, provide:

1. **The source code for solving the problem using MapReduce programming**
2. **The result of your code run in a separate text file (.txt). If more than 5 rows of results are**
    **available, you must report only the 5 first rows of such result.**


