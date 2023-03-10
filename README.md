# Instructions

The ETL notebook is available via https://github.com/gimenopea/missionwired/blob/34c6b7740f5f462775db9f3a4acff0559efc7c6b/PaulGimenoETLTask.ipynb

When running the etl_run.py locally, make sure to install python 3+. You can install dependencies through pip using requirements.txt or set-up a new environment using instructions below:
1. Create a new virtual environment by running the following command:

```{python}
  python -m venv env
```
This will create a new directory called "env" that contains the virtual environment.

2. Activate the virtual environment by running the following command:

On Windows:
```
  env\Scripts\activate.bat
```
On Unix or Linux:
```
  source env/bin/activate
```
3. Install the packages listed in the requirements.txt file by running the following command:

```
pip install -r requirements.txt
```
This will install all of the packages listed in the requirements.txt file in the virtual environment.

# Client Communication

Dear Client

I wanted to provide you with an update on the work I have been doing for your organization. Recently, I was tasked with processing a constituent file, a constituent email file, and a subscription file. To accomplish this, I used Python and the Pandas package, which is your organization's standard ETL methodology. There were several assumptions that I made during the processing of these files. First, I assumed that the cons_id field was consistent between the constituent file and the email file. Additionally, I assumed that one constituent may have multiple email addresses associated with them. Furthermore, I assumed that file sizes from the source application where the CSV files are derived from could be variable and large, and therefore I processed the files in chunks locally. It's worth noting that the approach could be different if processed through a distributed/data lake platform like Athena, Snowflake, or Databricks.

One of the key tasks I completed was to calculate the unsubscribed flags to the constituent ID level, and carried these flags across all emails if the primary email was still unsubscribed. The end result of the processing is a people.csv file, which lists the constituent create and last modified date, along with their email and unsubscribed flags. The purpose of the people.csv file is to generate an acquisitions file, which groups by date and the number of newly acquired emails at that date for chapter_id = 1. I'm confident that this data will be useful in informing your organization's strategy and decision-making.If you have any questions or concerns about the work that I have done, or would like to discuss the results further, please let me know. I am always available to chat and help out in any way that I can.

Best regards,
Paul

# Analytics Questions

Analytics part can be viewed in AnalyticsQuestions.ipynb