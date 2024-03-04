CREATE TABLE delay_flights (
    Id INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay DOUBLE,
    DepDelay DOUBLE,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted DOUBLE,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH "s3://flights-delay/dataset/DelayedFlights-updated.csv" INTO TABLE delay_flights;

INSERT OVERWRITE DIRECTORY 's3://flights-delay/hadoop-output/late_aircraft_delay/' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as Year_wise_late_aircraft_delay from delay_flights GROUP BY Year ORDER BY Year DESC;