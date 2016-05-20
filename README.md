## Motivation

According to the NYS DMV, between 2011 and 2013 there were 212,267 motor vehicle accidents in New York City, resulting in 833 deaths. In 2014, New York Mayor Bill de Blasio announced the Vision Zero Action Plan, with the aim of eliminating all traffic fatalities in New York City by 2025.
When the city debated the causes of these accidents and the most effective means to combat them, they turned to a dataset compiled by the DOT, which analyzed all NYC traffic crashes from 2008 to 2012.

For this project, I would like to examine this dataset from 2012 to 2013 along with traffic volume and vehicle classification data, and 311 complaints to identify areas around the city that are particularly hazardous for motor vehicles, and display common “symptoms” of these areas.

## Objective
The main objective of this project is to identify locations around the five boroughs that are prone to motor vehicle accidents by factoring in data like the number of accidents, fatalities, injuries, and number of vehicles involved (adjusting for traffic volume), and profiling these hazardous areas.

Tasks derived from objectives:
* Develop classification algorithm for classification of areas.
* Analyze hazardous areas for “symptoms”
  *  Analyze 311 complaints related to street/traffic conditions
  *  Analyze type of traffic (commercial, passenger, taxi)
* Plot areas labeled as hazardous, along with profile for area (most common vehicle type, most common 311 traffic/street condition related complaint)

See [Project Proposal](/Documents/Kasakyan_James_Project_Proposal.pdf) for more information.
NYS DMV New York City Crash Summaries https://dmv.ny.gov/org/about-dmv/statistical-summaries

## Instructions for spark submit:

``` $ spark-submit \
--name Group 3 Final Project \
--num-executors 64 \
--py-files police_reports.py,three_one_one.py,vehicle_volume_count.py \
main.py path/to/NYPD_Motor_Vehicle_Collisions.csv path/to/311_Service_Requests_from_2010_to_Present.csv \
path/to/output_from_script_generate_vehicle_count_csv ```


## Instructions for standard python:
``` $ python main.py path/to/NYPD_Motor_Vehicle_Collisions.csv path/to/311_Service_Requests_from_2010_to_Present.csv path/to/output_from_script_generate_vehicle_count_csv download```

*"download" is an optional fourth argument to main.py. Leave it blank if you do not wish to produce any output. All output will be saved in a directory called FinalProjectOutputs.*

***

## Datasets

[NYPD_Motor_Vehicle_Collisions](https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95)

[311_Service_Requests_from_2010_to_Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests/fvrb-kbbt)

[Traffic_Volume_Counts__2012-2013*](https://data.cityofnewyork.us/NYC-BigApps/Traffic-Volume-Counts-2012-2013-/p424-amsu)

**NOTE: This file is the input to generate_vehicle_count_count_csv.py. That script requires the
geopy package and the querying of the Nominatim geodatabase for ~900 records is lengthy. For convenience,
the output of that script is available in the Datasets folder.*

## Results

See [Final Report] (/Documents/Kasakyan_James_Final_Report.pdf)
View the [final mapping] (https://jameskasakyan.cartodb.com/viz/a612e2d2-1b34-11e6-a856-0e3ff518bd15/public_map) 
