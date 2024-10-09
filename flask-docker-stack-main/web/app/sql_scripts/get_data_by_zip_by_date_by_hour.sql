select w.*,
                    ns.name as station_name,
                    ns.distance,
                    ns.country_name
                    from
                    (
                                select station_id,
                                name,
                                distance,
                                country_name
                                from
                                (
                                SELECT 
                                station_id,
                                name,
                                country_name,
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
                                FROM weather_info.stations
								where year=%s
                                ) as a
                                where distance is not null
                                ORDER BY distance limit 1
                    ) as ns
                    inner join 
                    (
								 select 
								 station_id,
								 date(date) as given_date,
								 year(date) as given_year,
								 hour(date) as hour_of_day,
								 MONTH(date) as month_of_year,
								 WEEK(date) as week_of_month,
								 case when wind_direction_angle=999 then 'missing' else wind_direction_angle end as wind_direction_angle,
								 case when wind_direction_quality_code=0 then 'Passed gross limits check'
								 	when wind_direction_quality_code=1 then 'Passed all quality control checks'
								 	when wind_direction_quality_code=2 then 'Suspect'
								 	when wind_direction_quality_code=3 then 'Erroneous'
								 	when wind_direction_quality_code=4 then 'Passed gross limits check'
								 	when wind_direction_quality_code=5 then 'Passed all quality control checks'
								 	when wind_direction_quality_code=6 then 'Suspect'
								 	when wind_direction_quality_code=7 then 'Erroneous'
								 	when wind_direction_quality_code=9 then 'Passed gross limits'
								 	end as wind_direction_quality_code,
								case when wind_type_code='A' then 'Abridged Beaufort'
								 	when wind_type_code='B' then 'Beaufort'
								 	when wind_type_code='C' then 'Calm'
								 	when wind_type_code='H' then '5-Minute Average Speed '
								 	when wind_type_code='N' then 'Normal'
								 	when wind_type_code='R' then '60-Minute Average Speed'
								 	when wind_type_code='Q' then 'Squall'
								 	when wind_type_code='T' then '180 Minute Average Speed'
								 	when wind_type_code='V' then 'Variable'
								 	when wind_type_code=9 then 'Missing'
								 	end as wind_type_code,
								case when wind_speed_rate =9999 then 'missing' else wind_speed_rate end as wind_speed_rate,
								case when wind_speed_quality_code=0 then 'Passed gross limits check'
								 	when wind_speed_quality_code=1 then 'Passed all quality control checks'
								 	when wind_speed_quality_code=2 then 'Suspect'
								 	when wind_speed_quality_code=3 then 'Erroneous'
								 	when wind_speed_quality_code=4 then 'Passed gross limits check'
								 	when wind_speed_quality_code=5 then 'Passed all quality control checks'
								 	when wind_speed_quality_code=6 then 'Suspect'
								 	when wind_speed_quality_code=7 then 'Erroneous'
								 	when wind_speed_quality_code=9 then 'Passed gross limits'
								 	end as wind_speed_quality_code,
								case when sky_ceiling_height=99999 then 'missing' else sky_ceiling_height end as sky_ceiling_height,
								case when sky_ceiling_quality=0 then 'Passed gross limits check'
								 	when sky_ceiling_quality=1 then 'Passed all quality control checks'
								 	when sky_ceiling_quality=2 then 'Suspect'
								 	when sky_ceiling_quality=3 then 'Erroneous'
								 	when sky_ceiling_quality=4 then 'Passed gross limits check'
								 	when sky_ceiling_quality=5 then 'Passed all quality control checks'
								 	when sky_ceiling_quality=6 then 'Suspect'
								 	when sky_ceiling_quality=7 then 'Erroneous'
								 	when sky_ceiling_quality=9 then 'Passed gross limits'
								 	end as sky_ceiling_quality,
								CASE sky_ceiling_determination
								        WHEN 'A' THEN 'Aircraft'
								        WHEN 'B' THEN 'Balloon'
								        WHEN 'C' THEN 'Statistically derived'
								        WHEN 'D' THEN 'Persistent cirriform ceiling (pre-1950 data)'
								        WHEN 'E' THEN 'Estimated'
								        WHEN 'M' THEN 'Measured'
								        WHEN 'P' THEN 'Precipitation ceiling (pre-1950 data)'
								        WHEN 'R' THEN 'Radar'
								        WHEN 'S' THEN 'ASOS augmented'
								        WHEN 'U' THEN 'Unknown ceiling (pre-1950 data)'
								        WHEN 'V' THEN 'Variable ceiling (pre-1950 data)'
								        WHEN 'W' THEN 'Obscured'
								        WHEN '9' THEN 'Missing'
								        ELSE 'Unknown' 
								    END AS sky_ceiling_determination,
								case  sky_ceiling_cavok_code
									when 'N' then 'No'
									when 'Y' then 'Yes'
									when '9' then 'Missing'
								end as 	sky_ceiling_cavok_code,
								case when vis_distance_dim=999999 then 'missing' else vis_distance_dim end as vis_distance_dim,
										case when vis_distance_quality=0 then 'Passed gross limits check'
											when vis_distance_quality=1 then 'Passed all quality control checks'
											when vis_distance_quality=2 then 'Suspect'
											when vis_distance_quality=3 then 'Erroneous'
											when vis_distance_quality=4 then 'Passed gross limits check'
											when vis_distance_quality=5 then 'Passed all quality control checks'
											when vis_distance_quality=6 then 'Suspect'
											when vis_distance_quality=7 then 'Erroneous'
											when vis_distance_quality=9 then 'Passed gross limits'
											end as vis_distance_quality,
										case viz_variability
											when 'N' then 'No'
											when 'V' then 'Variable'
											when '9' then 'missing'
											end as viz_variability,
										case when viz_quality_variability=0 then 'Passed gross limits check'
											when viz_quality_variability=1 then 'Passed all quality control checks'
											when viz_quality_variability=2 then 'Suspect'
											when viz_quality_variability=3 then 'Erroneous'
											when viz_quality_variability=4 then 'Passed gross limits check'
											when viz_quality_variability=5 then 'Passed all quality control checks'
											when viz_quality_variability=6 then 'Suspect'
											when viz_quality_variability=7 then 'Erroneous'
											when viz_quality_variability=9 then 'Passed gross limits'
											end as viz_quality_variability,
										case when air_temp='+9999' then 'missing' else air_temp end as air_temp,
										CASE 
												WHEN air_temp_quality = 0 THEN 'Passed gross limits check'
												WHEN air_temp_quality = 1 THEN 'Passed all quality control checks'
												WHEN air_temp_quality = 2 THEN 'Suspect'
												WHEN air_temp_quality = 3 THEN 'Erroneous'
												WHEN air_temp_quality = 4 THEN 'Passed gross limits check'
												WHEN air_temp_quality = 5 THEN 'Passed all quality control checks'
												WHEN air_temp_quality = 6 THEN 'Suspect'
												WHEN air_temp_quality = 7 THEN 'Erroneous'
												WHEN air_temp_quality = 9 THEN 'Passed gross limits check if element is present'
												WHEN air_temp_quality = 'A' THEN 'Data value flagged as suspect, but accepted as a good value'
												WHEN air_temp_quality = 'C' THEN 'Temperature and dew point received from Automated Weather Observing System (AWOS) are reported in whole degrees Celsius. Automated QC flags these values, but they are accepted as valid.'
												WHEN air_temp_quality = 'I' THEN 'Data value not originally in data, but inserted by validator'
												WHEN air_temp_quality = 'M' THEN 'Manual changes made to value based on information provided by NWS or FAA'
												WHEN air_temp_quality = 'P' THEN 'Data value not originally flagged as suspect, but replaced by validator'
												WHEN air_temp_quality = 'R' THEN 'Data value replaced with value computed by NCEI software'
												WHEN air_temp_quality = 'U' THEN 'Data value replaced with edited value'
												ELSE 'Unknown'
											END AS air_temp_quality,
										case when air_dew='+9999' then 'missing' else air_dew end as air_dew,
										CASE 
												WHEN air_dew_quality = 0 THEN 'Passed gross limits check'
												WHEN air_dew_quality = 1 THEN 'Passed all quality control checks'
												WHEN air_dew_quality = 2 THEN 'Suspect'
												WHEN air_dew_quality = 3 THEN 'Erroneous'
												WHEN air_dew_quality = 4 THEN 'Passed gross limits check'
												WHEN air_dew_quality = 5 THEN 'Passed all quality control checks'
												WHEN air_dew_quality = 6 THEN 'Suspect'
												WHEN air_dew_quality = 7 THEN 'Erroneous'
												WHEN air_dew_quality = 9 THEN 'Passed gross limits check if element is present'
												WHEN air_dew_quality = 'A' THEN 'Data value flagged as suspect, but accepted as a good value'
												WHEN air_dew_quality = 'C' THEN 'Temperature and dew point received from Automated Weather Observing System (AWOS) are reported in whole degrees Celsius. Automated QC flags these values, but they are accepted as valid.'
												WHEN air_dew_quality = 'I' THEN 'Data value not originally in data, but inserted by validator'
												WHEN air_dew_quality = 'M' THEN 'Manual changes made to value based on information provided by NWS or FAA'
												WHEN air_dew_quality = 'P' THEN 'Data value not originally flagged as suspect, but replaced by validator'
												WHEN air_dew_quality = 'R' THEN 'Data value replaced with value computed by NCEI software'
												WHEN air_dew_quality = 'U' THEN 'Data value replaced with edited value'
												ELSE 'Unknown'
											END AS air_dew_quality,
										case when atm_pressure=99999 then 'missing' else atm_pressure end as atm_pressure,
										case when atm_pressure_quality=0 then 'Passed gross limits check'
											when atm_pressure_quality=1 then 'Passed all quality control checks'
											when atm_pressure_quality=2 then 'Suspect'
											when atm_pressure_quality=3 then 'Erroneous'
											when atm_pressure_quality=4 then 'Passed gross limits check'
											when atm_pressure_quality=5 then 'Passed all quality control checks'
											when atm_pressure_quality=6 then 'Suspect'
											when atm_pressure_quality=7 then 'Erroneous'
											when atm_pressure_quality=9 then 'Passed gross limits'
											end as atm_pressure_quality
								 from weather_info.weather_hourly
                            where date(date)=%s and hour(date)=%s
                    ) as w 
                    on ns.station_id=w.station_id;