# train_delay_analysis_DE

Train delay analysis is the process of analyzing the factors that cause delays in train services and the impact of those delays on the transportation system and the people who rely on it. It involves collecting data on train schedules, arrivals, and departures, as well as information on the causes of delays, such as mechanical problems, weather conditions, and human factors.

# Objective and requirements
Develop end-to-end data solution to perform advanced analysis of the train delays. The solution must meet the following requirements:

Infrastructure provided via code (IaC approach)
Cloud storage - data lake and data warehouse
Create separate pipelines for historical data and attributes. The historical data pipeline must run every month, attribute file ingestion pipeline will be triggered ad-hoc.
Data transformations must be performed using software engineering principles (data documentation, testing) and in a way that data analysts can easily understand and modify transformations.
Create an interactive report.

# Solution


# Data modeling
Data modeling is done in dbt cloud. dbt allows using software engineering approaches for data modeling. It offers native tools for testing and documentation; data infrastructure (DWH schemas, tables, views) is created automatically. On the other hand, data transformation is done using SQL, lowering thus entry threshold. All artifacts are stored in a git repository allowing version control and distributed development.

In the current project, dbt cloud is used to stage historical delays data (03-dbt/models/staging): the landing table records are filtered on the latest insert_datetime value, type casting is done. An additional source of train station attributes is loaded a seed to 03-dbt/seeds. In the core of the solution (03-dbt/models/core), station/locations and date dimension tables are created. The latter was intended to be used for hierarchical filtering in the reporting tool but was abandoned for the most due to limitations of Google Data Studio. Four data marts are created, which aggregate delay times in different groups.

Full data lineage of the dbt project:

<div id="header" align="center">
  <img src="https://github.com/IliaSinev/train-delays-analytics/blob/main/00-documentation/dbt_lineage.JPG" width="1000"/>
</div>
