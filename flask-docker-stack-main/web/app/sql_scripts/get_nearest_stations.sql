select 
distinct
station_id,
station,
name,
country_code,
latitude,
longitude,
distance
from
(
SELECT 
w.*,
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
FROM weather_info.stations as w
where year=(select max(year) from weather_info.stations s )
) as a
where distance is not null
ORDER BY distance limit %s;