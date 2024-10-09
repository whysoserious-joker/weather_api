select 
avg(latitude) as latitude ,
avg(longitude) as longitude  
from zipcodes
where postal_code=%s;