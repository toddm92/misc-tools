"""
 jira_close_ta_issue.py

 Python 3.11.6
"""

import json
from google.cloud import bigquery
from jira import JIRA, JIRAError
from jira_ta_modules import Auth, Common


def jira_close_issues(jira_ids, client):
    """
     close resolved jira issues
    """

    for id in jira_ids:
        resp = client.transition_issue(id, transition='Done')

        output = {'id': id, 'status': 'Done'}
        print('Jira issue id {id} marked as {status}'.format(**output))

    return


def main():
    """
     do the work..
    """

    auth = Auth()
    bqclient = auth.google_auth()
    jiraclient = auth.jira_auth()

    bqquery = (
        'SELECT * '
        'FROM `cloudops-automation.cloud_compendium_notifier.db_instances_expiring_by_acct` '
        'ORDER BY account_id'
        ) 

    jiraquery = 'project = CLOUDOPSTA AND issuetype = Task AND resolution = Unresolved AND description ~ "RDS"'

    common = Common()
    bq_ids = common.bqlist_acct_ids(bqquery, bqclient)

    if not bq_ids:
        print("nothing to do")
        return

    acct_ids, jira_ids = common.jira_search_issues(jiraquery, bq_ids, jiraclient)

    if not jira_ids:
        print("no matches found")
    else:
        resp = jira_close_issues(jira_ids, jiraclient)

    return


if __name__ == "__main__":
    main()
