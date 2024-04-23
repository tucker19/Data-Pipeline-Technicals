SELECT AVG(vehicle_meter_value) AS average_vehicle_meter_value, 'mi' as vehicle_meter_unit
FROM (
  select 
    case when vehicle_meter_unit = 'km' then vehicle_meter_value*0.621371
    else vehicle_meter_value
    end as vehicle_meter_value,
    case when vehicle_meter_unit = 'km' then 'mi'
    else vehicle_meter_unit
    end as vehicle_meter_unit
  from issues
  where vehicle_meter_unit  is not null
  and vehicle_meter_unit in ('km', 'mi')
);

select AVG(vehicle_meter_value) AS average_vehicle_meter_value, vehicle_meter_unit
from issues
where vehicle_meter_unit  is not null
and vehicle_meter_unit in ('km', 'mi')
group by vehicle_meter_unit;
