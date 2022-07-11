#Bonus Queries
#From the two most commonly appearing regions, which is the latest datasource? 
#To do that I did a subquery to extract the latest datasource and then I left joined it with the two mostly appearing regions by counting and ordering the trips and taking the first two rows.

SELECT 
	TripsRegionCount.region,
	COUNT(*) as TripsCount,
	TripsRegionCount.datasource
FROM
	trips as TripsRegionCount
LEFT JOIN (
		SELECT DISTINCT
			TRIPS.region,
			TRIPS.datasource
		FROM 
			trips AS TRIPS
		INNER JOIN (
			SELECT 
				region, 
				MAX(datetime) as latest_date
			FROM 
				trips
			GROUP BY 
				region
		) AS LATEST
		ON
			TRIPS.region = LATEST.region and TRIPS.datetime = LATEST.latest_date
	) AS LATEST_datasource
ON
	TripsRegionCount.region = LATEST_datasource.region
GROUP BY
	TripsRegionCount.region,
	TripsRegionCount.datasource
ORDER BY 
	TripsCount DESC
LIMIT 2

#What regions has the "cheap_mobile" datasource appeared in? 
#In this query I extracted the region and data source,  where datasource equals cheap_mobile and grouped it by region and datasource.
SELECT
	region,
	datasource
FROM
	public.trips
WHERE
	datasource = 'cheap_mobile'
GROUP BY
	region,
	datasource

