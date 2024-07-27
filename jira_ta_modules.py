"""
 jira_ta_modules.py

 Python 3.11.6
"""

import os
import re
from google.cloud import bigquery
from jira import JIRA


class Auth():

    def google_auth(self):
        """
         google client
        """

        bqclient = bigquery.Client(project='cloudops-automation')
        ### gcloud auth application-default login ###

        return bqclient


    def jira_auth(self):
        """
         jira client
        """

        try:
            username=os.environ['JIRA_USER']
            password=os.environ['JIRA_PASS']
        except KeyError as e:
            print("check jira os environment variables..")
            return
        else:
            jiraclient = JIRA(server='http://localhost:8081/', basic_auth=(username, password))

        return jiraclient


class Common():

    def jira_search_issues(self, query, bq_ids, client):
        """
         search jira TA issues; return AccountIds and jiraIds not found in BQ
        """

        issue = client.search_issues(jql_str=query, fields=['id', 'key', 'description'], maxResults=100, json_result=True)

        #print(json.dumps(issue['issues'], indent=4))
        jiradata = issue['issues']

        acct_ids = []
        jira_ids = []

        pattern = r'AccountId: (\w+)'

        for l in jiradata:
            d = dict(l)
            f = d.get("fields")
            match = re.findall(pattern, f.get("description"))
            if match:
                acct_ids.append(match[0])
                if match[0] not in bq_ids:
                    jira_ids.append(d.get("id"))

        return acct_ids, jira_ids


    def bqlist_acct_ids(self, query, client):
        """
         create a list of AccountIds from BQ data
        """

        query_bq = client.query(query)
        query_data = query_bq.result()

        bq_ids = []

        for row in query_data:
            bq_ids.append(row.account_id)

        return bq_ids
