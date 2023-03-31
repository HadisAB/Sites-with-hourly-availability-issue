# Sites-with-hourly-availability-issue
The main goal of this project is to find sites with 2G/ 3G/ 4G-FDD or 4G-TDD hourly availability issues in network as soon as possible to prevent revenue loss.

## Method
As availability is the main KPI of each site and we cannot use a site unless it is available, I decided to use the hourly availability KPIs per site in whole network to find the degraded sites asap. <br/>
Please be informed the input files are template and you should replace your data on them to be able to use the script.
- Export hourly stats from databases or other tools.
- Preprocessing the stats and make them ready for applying the anomaly detection method.
- Providing targets and find the abnormal behaviours among the site's KPI.
- finding the number of hours in which the site had problem till now.
- Compare exported sites with a tracker to find the delay of sites per day and raise the long-pending cases.
- I do extra investigation on the categorizing cases and share them on relavnt teams (excluded from script).
- I provide some managemnet level plots for tracking the performance of sites per different categories and different area in the country(excluded from script).
- Exporting the result in an excel file.
- Share result with responsible teams via automated email.

## Output
The Output is an excel file consising a list of degraded sites, the 'Aginng' , 'Category; and other extra information which will help the responsible teams to trace the issue more comfortable. 

Example of detected cases with this algorithm:
<img src=  />


