select 
w.station_id ,
w.station ,
w.latitude,
w.longitude,
w.name,
w.country_code ,
avg(CASE WHEN w.wind_direction_angle <> 999 THEN  w.wind_direction_angle END ) as wind_direction_angle,
round(avg(CASE WHEN w.wind_speed_rate  <> 9999 THEN  w.wind_speed_rate END ),2) as wind_speed_rate,
round(max(CASE WHEN w.wind_speed_rate  <> 9999 THEN  w.wind_speed_rate END ),2) as wind_speed_rate_max,
round(avg(CASE WHEN w.sky_ceiling_height  <> 99999 THEN  w.sky_ceiling_height END ),2) as sky_ceiling_height,
round(avg(CASE WHEN w.vis_distance_dim  <> 999999 THEN  w.vis_distance_dim END ),2) as vis_distance_dim,
round(avg(CASE WHEN w.air_temp  <> 9999 THEN  w.air_temp END ),2) as air_temp,
round(max(CASE WHEN w.air_temp  <> 9999 THEN  w.air_temp END ),2) as air_temp_max,
round(min(CASE WHEN w.air_temp  <> 9999 THEN  w.air_temp END ),2) as air_temp_min,
round(avg(CASE WHEN w.air_dew  <> 9999 THEN  w.air_dew  END ),2) as air_dew,
round(avg(CASE WHEN w.atm_pressure  <> 99999 THEN  w.atm_pressure  END ),2) as atm_pressure
from weather_info.weather_hourly w
where date(date) between %s and %s and station_id = (
						select station_id
		                                from
		                                (
		                                SELECT 
		                                station_id,
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
		                                ) )as distance
		                                FROM stations
		                                where year=(select GREATEST(%s,%s))
		                                ) as a
		                                where  distance is not null
		                                ORDER BY distance limit 1
)
group by 1,2,3,4,5,6;