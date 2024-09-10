# ODB Open API to query WOA 2023 (WOA23) API

[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.13739802.svg)](https://doi.org/10.5281/zenodo.13739802)

#### Swagger API doc

[ODB WOA23 API manual/online try-out](https://api.odb.ntu.edu.tw/hub/swagger?node=odb_woa23_v1)

#### Usage

1. Query WOA23 data (in JSON)

* e.g. https://eco.odb.ntu.edu.tw/api/woa23?lon0=125&lat0=15&dep0=100&grid=1&parameter=temperature,salinity&time_period=13,14,15,16
   
* Please refer to WOA2023 official documentation (in PDF) to find the details about depth level, parameters, and corresponding time period definition: https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf
   
2. Query WOA23 data (in CSV)

* API endpoint: api/woa23/csv and all the same parameters as api/woa23

#### Demo 

[![Demo_by_WOA23_API](https://github.com/cywhale/woa23/blob/main/figs/salinity_profile_woa23_annual01.png?raw=true)](https://github.com/cywhale/woa23/blob/main/figs/salinity_profile_woa23_annual01.png)<br/>
*Use WOA 2023 annual mean data queried by ODB WOA23 API to plot the sea salinity profile along 22°N around Taiwan waters.*

[![Demo_by_WOA23_API](https://github.com/cywhale/woa23/blob/main/figs/temperature_map_woa23_annual01.png?raw=true)](https://github.com/cywhale/woa23/blob/main/figs/temperature_map_woa23_annual01.png)<br/>
*Use WOA 2023 annual mean data queried by ODB WOA23 API to plot world-wide sea temperature distribution.*

#### Data source

  - Data fetched and compiled from [The World Ocean Atlas (WOA) 2023](https://www.ncei.noaa.gov/products/world-ocean-atlas).
 
       - Reagan, James R.; Boyer, Tim P.; García, Hernán E.; Locarnini, Ricardo A.; Baranova, Olga K.; Bouchard, Courtney; Cross, Scott L.; Mishonov, Alexey V.; Paver, Christopher R.; Seidov, Dan; Wang, Zhankun; Dukhovskoy, Dmitry. (2024). World Ocean Atlas 2023. NOAA National Centers for Environmental Information. Dataset: NCEI Accession 0270533.
         
#### Citation

* This API is compiled by [Ocean Data Bank](https://www.odb.ntu.edu.tw) (ODB), and can be cited as:

    * Ocean Data Bank, National Science and Technology Council, Taiwan. https://doi.org/10.5281/zenodo.7512112. Accessed DAY/MONTH/YEAR from eco.odb.ntu.edu.tw/api/woa23. v1.0.
