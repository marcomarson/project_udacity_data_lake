# Udacity Project 4

Project: Data Lake


## Motivation

A company called Sparkify needs a easier/faster way to access their data, they need to see and do the analytics of their users activities on their streaming app.
#copied from my motivation written for Project Data Modeling with postgres


## Method and results

First, to solve the problem that Sparkify is facing, a star schema was selected with only the data that they need for their analytics.

The main purpose of this project was to learn how to manipulate data from s3 using EMR(spark in this case).

## ETL Process

There are not much to say here, what was done in this project is that we need to infer the data from s3 to a spark dataframe and after that, using  or spark dataframesto insert the data into another table with the starschema. I tried to complete this project using only pyspark dataframe, and not SQL.
After finishing everything, the analytics will be able to use the data in a simple way, with at maximum 4 joins to get all the data to see the user's patterns.

## Repository overview

├── README.md
├── my-notebook.ipynb
└── dl.cfg ( cfg file added on .gitignore, so it won't appear here)
└── etl.py



## Running instructions

To use this project, call etl.py. Ensure to have the EMR running and the credentials changed into the dwh.cfg and change the s3 output as well.
After this process finishes, the tables will be in parquet mode in s3 available for querying.


## About

This project was done by Marco Marson for the Data Engineering NanoDegree at Udacity in Fev 2023.

