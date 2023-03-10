# Water_Quality_Project
## Background
The safe to swim program was developed to let the public know which public waters are safe for recreational use. This program is based on testing the waters regularly for certain bacteria and chemical markers. The five markers of interest for this project are: E. Coli, Enterococci, Total Coliform, Fecal Coliform, and HF183. Water Quality Objectives (WQO’s) are maximum amounts of the specific markers that can be present in a given body of water to be considered safe for use. These bodies of water are tested regularly to determine whether they meet the requirements. The sources for WQO’s and testing requirements can be found in the following sources:
-	[Ocean Plan](https://www.waterboards.ca.gov/water_issues/programs/ocean/docs/oceanplan2019.pdf)
- [Basin Plan](https://www.waterboards.ca.gov/sandiego/water_issues/programs/basin_plan)
- [ISWEBE Plan](https://www.waterboards.ca.gov/plans_policies/docs/bacteria.pdf)

Basically, there exist certain rolling thresholds calcualted above which waters are considered not safe.  The key take aways are:
- One aspect of determining recreational water safety is by testing the water regularly for certain markers, including: E. Coli, Enterococci, Total Coliform, Fecal Coliform, and HF183.
- Water Quality Objectives (WQO’s) and testing requirements are outlined in the various water quality plans
- Certain measurements are used to determine whether there is an exceedance of these markers: single sample maximums, geometric/arithmetic means, and statistical threshold values.
- If results indicate these WQO’s are not being met, then the water is not safe for recreational use.
 
## Work done
The CEDEN database is the main source of truth for these chemical tests. CEDEN has a lot of columns stored within, but the main columns of interest for this analysis can be seen in the scripts themselves. The scripts were broken into four parts:
- data_transformation_historical
- ca_open_data_api_2020
- data_transformation_update
- geo_mean

The first script, data_transformation_historical, requires the URL’s for the csv’s of the three databases as given on the [California Open Data Source](https://data.ca.gov/dataset/surface-water-fecal-indicator-bacteria-results). The data is then processed by: 
- Changing the datatypes/removing null values (no extrapolation is done, as this analysis must be based on actual values)
- Calculating the geometric means (which is done in the geomean script)
- Mapping the correct exceedance logic
These are then combined into one csv file to be saved on the network. From then on, the data_transformation_update script is then run on a to be determined basis (weekly or bi-weekly is probably optimal). This will call ca_open_data_api_2020, which handles the API calls and queries the new data. The data_transformation_update script then does the same data transformations, recalculates all statistical values, and appends this data to the csv. This could be easily repointed to a database.

As of June 2022, all the scripts were written in Python 3.9.4. The required packages are 
- Requests
- Pandas
- Numpy

## Future Work
Future work to be done (in order of importance):
- Import into PowerBI
- Add geographic boundaries (currently running for entire dataset/state/?)
- Calculate STV (need to refer to documentation, one possibly methodology is found [here](https://producesafetyalliance.cornell.edu/sites/producesafetyalliance.cornell.edu/files/shared/documents/2017%20GM%20STV%20Worksheet%20v1.0.pdf))
- Add HF183 (possibly found [here](https://data.ca.gov/dataset/surface-water-chemistry-results))
- Add wet/dry weather data (?)

