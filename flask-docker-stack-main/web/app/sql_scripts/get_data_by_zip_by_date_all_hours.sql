WITH closest_station AS (
    SELECT 
        station_id,
        name as station_name,
        2* atan2 ( sqrt(
		                                sin(radians(latitude-(%s))/2) * 
		                                sin(radians(latitude-(%s))/2) +
		                                cos(radians(44.6592)) *
		                                cos(radians(latitude)) *
		                                sin(radians(longitude-(%s))/2) * 
		                                sin(radians(longitude-(%s))/2) 
		                                ),1-(
		                                sin(radians(latitude-(%s))/2) * 
		                                sin(radians(latitude-(%s))/2) +
		                                cos(radians(44.6592)) *
		                                cos(radians(latitude)) *
		                                sin(radians(longitude-(%s))/2) * 
		                                sin(radians(longitude-(%s))/2) 
		                                ) )
         AS distance
    FROM stations
    WHERE year = %s
    ORDER BY distance
    LIMIT 1
),
weather_data AS (
    SELECT 
        station_id,
        (select station_name from closest_station) station_name,
		(select distance from closest_station) distance,
        DATE(date) AS user_date,
        YEAR(date) AS given_year,
        HOUR(date) AS hour_of_day,
        ROUND(AVG(NULLIF(wind_direction_angle, 999)), 2) AS avg_wind_direction_angle,
        ROUND(AVG(NULLIF(wind_speed_rate, 9999)), 2) AS avg_wind_speed_rate,
        ROUND(AVG(NULLIF(sky_ceiling_height, 99999)), 2) AS avg_sky_ceiling_height,
        ROUND(AVG(NULLIF(vis_distance_dim, 999999)), 2) AS avg_vis_distance,
        ROUND(AVG(NULLIF(air_temp, '+9999')), 2) AS avg_air_temp,
        ROUND(AVG(NULLIF(air_dew, '+9999')), 2) AS avg_air_dew,
        ROUND(AVG(NULLIF(atm_pressure, 99999)), 2) AS avg_atm_pressure
    FROM weather_info.weather_hourly
    WHERE DATE(date) = %s
    AND station_id = (SELECT station_id FROM closest_station)
    GROUP BY 1,2,3,4,5,6
)
SELECT 
    wd.station_id,
    wd.station_name,
    round(wd.distance,3) as distance,
    wd.user_date,
    wd.given_year,
    wd.hour_of_day,
    wd.avg_wind_direction_angle,
    wd.avg_wind_speed_rate,
    wd.avg_sky_ceiling_height,
    wd.avg_vis_distance,
    wd.avg_air_temp,
    wd.avg_air_dew,
    wd.avg_atm_pressure
FROM weather_data wd;