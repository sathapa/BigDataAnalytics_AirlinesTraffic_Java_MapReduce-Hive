SELECT d.IATA_code,d.delayedFlights_15,d.totalFlights,
d.delayedFlights_15/d.totalFlights*100 AS "delay_Rate",
a.airport,a.city,a.state,a.country,a.latitude,a.longitude 
from delayed15 d inner join airports a 
where d.IATA_code=a.IATA_code
#ORDER BY CAST(delay_Rate as SIGNED INTEGER) DESC;
ORDER BY CAST(delayedFlights_15 as SIGNED INTEGER) DESC
