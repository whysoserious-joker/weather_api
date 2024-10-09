select 
country_code,
place_name,
avg(latitude) as latitude ,
avg(longitude) as longitude  
from weather.zipcodes_info
where lower(place_name) LIKE %s and lower(country_code)= %s
group by 1,2;