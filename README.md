# Project Name: AA_TASK

## 1) Introduction:

This project/task is intend to do some quality checks on given data based on some requirements.

* For findings please check 'steps_findings' from project directory under AA_Task/findings_reports
* A notebook is prepared (under AA_Task/findings_reports), for all analysis done on data, checking that document would
  be the best way to understand the codes as well as the results

## 2) Requirements:

### Project Tools/Libraries

- Python
- Pytest
- Pytest-bdd
- Pyspark
- Pandas
- Allure-pytest (for report)

## 3) Installation:

- Choose pytest as default test runner from Preferences/tools/Python Integrated Tools
- To create virtual environment follow these steps, from terminal run:
    - python3 -m venv venv
    - source venv/bin/activate
    - make env
      (that will install everything from requirements.txt)

- for report follow these steps
    - install allure to your env
      (for documentation: https://docs.qameta.io/allure/)
      (for mac, from terminal run brew install allure)
    - to check allure is installed from terminal run: allure --version
    - in project directory run: allure generate   
      (that will generate a folder for reports)
    - while running pytest from command line add:
      --alluredir=allure-report/ to the end (ex: pytest -k tagname --alluredir=allure-report/)
    - to check the reports run this command:
      allure serve allure-report/

## 4) Data Requirements

### scrape_appearances

The Scrape appearances bucket contains a number of scrapes we have made for the search term on specific device and date.

#### Columns:

date: Date (YYYY-MM-DD). device: String. search_term: String scrape_count: Int

#### Constraints:

Primary Key (unique): date, device, search_term. device (non-NULL): only mobile and desktop are supported. search_term (
non-NULL): maximum 400 characters long. scrape_count (non-NULL): > 0.

### competitor_appearances

The Competitor appearances bucket contains advertiser (domain) statistics from the scrapes that we have made for a given
search term, on a given day and device. Each row in this dataset must have a corresponding row in the scrape appearances
dataset.

#### Columns:

date: Date (YYYY-MM-DD). device: String. search_term: String domain: String sponsored_appearances: Int
natural_appearances: Int pla_appearances: Int ctr: Double

#### Constraints:

Primary Key (unique): date, device, search_term, domain. device (non-NULL): only mobile and desktop are supported.
search_term (non-NULL): maximum 400 characters long. domain (non-NULL): maximum 100 characters long.
sponsored_appearances (non-NULL): Minimum value: 0. Maximum value: the scrape_count value from scrape appearances for
that search term, device and date. natural_appearances (non-NULL): Minimum value: 0. Maximum value: unlimited.
pla_appearances (non-NULL): Minimum value: 0. Maximum value: unlimited. ctr (optional - NULLable): Minimum value: 0.
Maximum value: 1.0.

## 5) How to Run

- to run any test: pytest -k tagname -vv -v -s (to generate reports add: --alluredir=allure-report)

## 6) Findings

### What-I-found-report.txt

- Check findings_reports/steps_findings.txt file under Project directory
- That is generated manually as the tests already fails in early stages.
- for more details please check the notebook.

### AA_Task.ipynb

- All analysis done on data, can be found on here.
- The assertions commented out as that is for analysis purposes.

# AA_Task
