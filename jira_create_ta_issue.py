"""
 jira_create_ta_issue.py

 Python 3.11.6
"""

import json
from google.cloud import bigquery
from jira import JIRA, JIRAError
from jira_ta_modules import Auth, Common


def jira_create_fields(query, acct_ids, client):
    """
     create jira issue fields from BQ data
    """

    query_bq = client.query(query)
    query_data = query_bq.result()

    bqlist = []
    nl = "\n"

    for row in query_data:
        if row.account_id not in acct_ids:

            entry =  {
                       "project": {
                         "id": "10100"
                       },
                       "summary": f"AWS lambda deprecated runtimes: {row.idname}",
                       "description": f"[ *Action Required* ] {nl} \
                                        AccountId: {row.account_id} {nl}{nl} \
                                        Fix this crap now!",
                       "issuetype": {
                         "name": "Task"
                       },
                       "update": {
                         "watchers": f"{row.user_contact}",
                         "resources": f"{row.resources}"
                       }
                     }

            bqlist.append(entry)
        break

    return bqlist


def jira_create_issue(bqdata, client):
    """
     create jira TA issues
    """

    jira_run = []
    nl = "\n"
    affected = f"[ *Affected Resources* ] {nl}"

    for fields in bqdata:
        update = fields.pop("update", None)

        # create jira issue
        issue = client.create_issue(fields=fields)

        # add comment to jira issue
        resources = update["resources"].split(",")
        for resource in resources:
            affected = affected + f"{nl}{resource}"
            
        resp = client.add_comment(issue=issue, body=affected)

        # add watchers to jira issue
        watchers = update["watchers"].split(",")
        for watcher in watchers:
            try:
                resp = client.add_watcher(issue=issue, watcher=watcher)
            except JIRAError as e:
                print(e.text)
            else:
                print(f"added watcher {watcher}")

        jira_run.append(issue)

    return jira_run


def main():
    """
     do the work..
    """

    auth = Auth()
    bqclient = auth.google_auth()
    jiraclient = auth.jira_auth()

    bqquery = (
        'SELECT * '
        'FROM `cloudops-automation.cloud_compendium_notifier.lambdas_deprecated_by_acct` '
        'ORDER BY account_id'
        ) 

    jiraquery = 'project = CLOUDOPSTA AND issuetype = Task AND resolution = Unresolved AND summary ~ "lambda"'

    common = Common()
    bq_ids = common.bqlist_acct_ids(bqquery, bqclient)
    acct_ids, jira_ids = common.jira_search_issues(jiraquery, bq_ids, jiraclient)

    bqdata = jira_create_fields(bqquery, acct_ids, bqclient)

    if not bqdata:
        print("nothing to do")
        return

    issues = jira_create_issue(bqdata, jiraclient)
    print(issues)

    return


if __name__ == "__main__":
    main()
