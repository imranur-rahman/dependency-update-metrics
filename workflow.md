I need to build an python tool that can be run as a file with arguments or the packaged tool can be run with arguments. The packaged tool needs to be able to publishable to pypi.

Arguments:
--ecosystem : the name of the ecosystem, e.g., npm, pypi
--package : the name of the package
--start-date : packages and dependencies released version will be considered if that is after the start-date; default start date is '1900-01-01'
--end-date : packages and dependencies released version will be considered if that is before the end-date; default is datetime.today
--weighting-type : either linear, exponential, inverse or disable
--half-life : in days, (optional arg), only use when weighting-type is exponential
--build-osv : if we need to create the osv-extended df
--get-osv : return the osv dataset for the ecosystem and vulnerable dependencies of the package.
--get-worksheets : if you need to get the dfs for all dependencies of the package in a excel file with multiple sheets.

plan to create vuln database: (if --build-osv flag is on), take inspiration from build-db-from-osv-ipynb file:
1. pull data from osv for the all ecosystems: https://storage.googleapis.com/osv-vulnerabilities/all.zip
2. unzip and for each json file, populate vul_id, ecosystem, package, vul_introduced, vul_fixed columns of a df.
3. If one cve contains multiple vulnerable ranges and one fixed version for each vulnerable ranges, populate into multiple rows.


Plan for npm ecosystem but keep it general so that we can incorporate for pypi easily:
1. pull the package metadata from (https://registry.npmjs.org/<package_name>).
2. extract the dependencies and their constraints from the json file for the package's version which is the closest to the end-date and before the end date.
3. pull the metadata for dependencies from (https://registry.npmjs.org/) and keep their released versions and released dates.
4. For each package-dependency pair, we consider a timeline from start-date to end-date and will fill up a dataframe with columns {ecosystem, package, package_version, dependency, dependency_constraint, constraint_type, dependency_version, dependency_highest_version, interval_start, interval_end, updated, remediated}.
5. For each package-dependency pair, the interval_start dates with be when either the package or dependency released a new version and time intervals are continuous: for one rows interval_end will be the interval_start for the next row. The time interval range is like [interval_start, interval_end).
6. we will then populate dependency_version by calling npm cli tool with --before <interval_start> to resolve the dependency constraint for that interval
 7. We will also populate dependency_highest_version as the highest available semver release from the dependency at interval_start.
8. updated = true if dependency_version == dependency_highest_version; otherwise false.
9. take a subset of osv-extended table for the ecosystem and dependency as package, add another column fixed_version_release_date, and populate with dependencies metadata.
10. remediated = true if the dependency_version is within a [vul_introduced, vul_fixed) range and the fixed_version_release_date > interval_start; otherwise false
11. add a column age_of_interval, which is the difference between max(interval_start) and interval_start.
12. add a column weight if weighting is enabled, and compute weight for each row based on age_of_interval. For exponential it will be exp(-lambda * age_of_interval) and lambda = (ln 2)/half_life. And similarly for linear and inverse.
13. to compute time-to-update or ttu for this package-dependency pair, (a) if weighting enabled: use (summation of weight*interval_duration) / (summation of weights) for the rows where updated=false; (b) if disabled, use summation of interval duration of rows with updated=false.
14. do the same to compute time-to-remediate or ttr but only use remediated column.
15. do the above process for each dependency of the pacakge and average the ttu and ttr by the number of dependencies and return those two values in a json.
16. return the osv-minified or the constructed df if corresponding flag is on.
