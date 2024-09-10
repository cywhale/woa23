# WOA23 API

#### Swagger API doc

[ODB WOA23 API manual/online try-out](https://api.odb.ntu.edu.tw/hub/swagger?node=odb_woa23_v1)

#### Usage

1. Query WOA23 data (in JSON)

* e.g. https://eco.odb.ntu.edu.tw/api/woa23?lon0=125&lat0=15&dep0=100&grid=1&parameter=temperature,salinity&time_period=13,14,15,16
   
* Please refer to WOA2023 official documentation (in PDF) to find the details about depth level, parameters, and corresponding time period definition: https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf
   
2. Query WOA23 data (in CSV)

* API endpoint: api/woa23/csv and all the same parameters as api/woa23

#### Data source

  - Data fetched and compiled from [The World Ocean Atlas (WOA) 2023](https://www.ncei.noaa.gov/products/world-ocean-atlas).
 
       - Reagan, James R.; Boyer, Tim P.; García, Hernán E.; Locarnini, Ricardo A.; Baranova, Olga K.; Bouchard, Courtney; Cross, Scott L.; Mishonov, Alexey V.; Paver, Christopher R.; Seidov, Dan; Wang, Zhankun; Dukhovskoy, Dmitry. (2024). World Ocean Atlas 2023. NOAA National Centers for Environmental Information. Dataset: NCEI Accession 0270533.
