# databootcamp-capstone

## Goal

Process and analyze bike share data to optimize bike distribution and plan optimal routes for bike placement and re-distribution

## Data

We will focus, for this project, on Citi Bike data from NYC, from 2024 to current.

[Main Data Source Info Page](https://citibikenyc.com/system-data)
[Main Data Source File Downloads](https://s3.amazonaws.com/tripdata/index.html)

To use the data collection script:

    To get the most recent month of data:

    ```
    python3 scripts/collectCitiBikeNycData.py
    ```

    To get all data starting from the beginning of 2024:

    ```
    python3 scripts/collectCitiBikeNycData.py --backfill
    ```

    To download a specific month of data:
    ```
    python3 scripts/collectCitiBikeNycData.py --url https://s3.amazonaws.com/tripdata/202403-citibike-tripdata.csv.zip
    ```