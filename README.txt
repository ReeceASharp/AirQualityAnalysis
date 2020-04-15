Author: Reece Sharp
Project: ANALYZING AIR QUALITY DATA COLLECTED ACROSS THE UNITED STATES USING MAPREDUCE

Quick Start:
1. Untar and go into new directory
    'tar -xvf Reece-Sharp-HW3-PC.tar'
    'cd Reece-Sharp-HW3-PC'
2. To build:
    'gradle build'
3. To run:
    '$HADOOP_HOME/bin/hadoop jar build/libs/hw3.jar cs455.hadoop.PACKAGE /PATH/TO/DATA /PATH/TO/OUTPUT


File Structure:
******
|-src
    |-main
        |-java
            |-cs455
                |-hadoop
                    |-q_one
                        |- SiteCountJob.java
                        |- SiteMapperOne.java
                        |- SiteMapperTwo.java
                        |- SiteReducerOne.java
                        |- SiteReducerTwo.java
                    |-q_two
                        |- EWSO2Mapper.java
                        |- EastWestMapper.java
                        |- EastWestReducer.java
                    |-q_three
                        |- DaySO2.java
                        |- DaySO2Mapper.java
                        |- DaySO2Reducer.java
                    |-q_four
                        |- YearSO2Job.java
                        |- YearSO2Reducer.java
                        |- YearSO2Mapper.java
                    |-q_five
                        |- TopHotJob.java
                        |- TopHotMapper.java
                        |- TopHotReducer.java
                    |-q_six
                        |- HotMeanSO2Job.java
                        |- HotMeanSO2Mapper.java
                        |- HotMeanSO2Reducer.java
                    |-util
                        |- DoubleComparator.java
                        |- DoubleSortMapper.java
                        |- DoubleSortReducer.java
                        |- IntComparator.java
                        |- IntSortMapper.java
                        |- IntSortReducer.java
|- build.gradle
|- README.txt
|- cs455_report.pdf

File Descriptions
******
|- SiteCountJob.java
    - Hadoop driver for Q1. runs 3 jobs (2 + sort)
|- SiteMapperOne.java
    - First mapper for Q1, maps each unique site combination to 1
|- SiteReducerOne.java
    - First reducer for Q1, condenses the unique sites together
|- SiteMapperTwo.java
    - Second mapper for Q1, condenses all the site values
|- SiteReducerTwo.java
    - First reducer for Q1, condenses the unique sites together

|- EWSO2Mapper.java
    - Hadoop driver for Q2, finds avg SO2 values for Esat States, and West states
|- EastWestMapper.java
    - Mapper for Q2, maps the data to either East states, or West states (if applicable)
|- EastWestReducer.java
    - Reducer for Q3, finds average SO2 for East and West states

|- DaySO2.java
    - Hadoop driver for Q3, finds avg SO2 values across the day (each hour)
|- DaySO2Mapper.java
    - Mapper for Q3, maps the SO2 data to each hour
|- DaySO2Reducer.java
    - Reducer for Q3, averages out the SO2 data for each hour

|- YearSO2Job.java
    - Hadoop driver for Q4, finds avg SO2 values across each each 1980-2019
|- YearSO2Mapper.java
    - Mapper for Q4, maps SO2 values to each year
|- YearSO2Reducer.java
    - Reducer for Q3, finds avg SO2 for each year

|- TopHotJob.java
    - Hadoop driver for Q5, finds top hottest states (Fahrenheit), sorts
|- TopHotMapper.java
    - Mapper for Q5, Maps temperature values to each State
|- TopHotReducer.java
    - Reducer for Q3, finds avg temperature for each state

|- HotMeanSO2Job.java
    - Hadoop driver for Q6, finds SO2 valuse for top ten hottest states found in TopHotJob.java, sorts
|- HotMeanSO2Mapper.java
    - Mapper for Q6, maps the SO2 values for each of the 10 hottest states
|- HotMeanSO2Reducer.java
    - Reducer for Q3, finds avg of SO2 value for each hot state

|- DoubleComparator.java
    - Used by jobs that are working with Doubles to organize data in descending order
|- DoubleSortMapper.java
    - Used in Double sorting, flips values to keys and vice versa
|- DoubleSortReducer.java
    - Used in Double sorting, just outputs the data again, needed for comparator to work
|- IntComparator.java
    - Used by jobs that are working with Ints to organize data in descending order
|- IntSortMapper.java
    - Used in Int sorting, flips values to keys and vice versa
|- IntSortReducer.java
    - Used in Int sorting, just outputs the data again, needed for comparator to work              
|- build.gradle
    - File utilized by gradle to build the cs455 package
|- cs455_report.pdf
    - documents the results of the hadoop programs Q1-6