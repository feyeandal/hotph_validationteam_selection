import requests
import json
import csv
import pandas as pd
from pandas import json_normalize

d = pd.read_csv('users.csv')

appended_data = []

# get all projects of qualified users
print("get all projects of qualified users...")
for username in d['username']:
    url = ("https://tasking-manager-tm4-production-api.hotosm.org/api/v2/projects/queries/" + username + "/touched/")
    
    response = requests.get(url)
    data = response.text
    parsed = json.loads(data)

    df = json_normalize(parsed, 'mappedProjects')

    df2 = df[['projectId', 'name', 'tasksValidated']]
    df2['username'] = username

    indexNames = df2[df2['tasksValidated'] != 0]

    appended_data.append(indexNames)

appended_data = pd.concat(appended_data)
appended_data.to_csv("projects.csv", index=False, encoding='utf-8')

e = pd.read_csv('projects.csv')

final_append = []

# get all grids from the projects validated by users
print("get all grids from the projects validated by users...")
for iD in e['projectId']:
    url_2 = ("https://tasking-manager-tm4-production-api.hotosm.org/api/v2/projects/" + str(iD) + "/contributions/")

    response_2 = requests.get(url_2)

    data_id = response_2.text
    parsed_id = json.loads(data_id)

    df_id = json_normalize(parsed_id, 'userContributions')

    df_of_id = df_id[['username', 'validatedTasks']]
    df_of_id['id'] = iD

    df3 = pd.merge(appended_data, df_of_id, how='inner', left_on=['projectId','username'], right_on=['id', 'username'])

    final_append.append(df3)

final_append = pd.concat(final_append)

test = final_append.explode('validatedTasks')
test.drop(['id'], axis=1, inplace=True, errors='ignore')
renamed_df = test.rename(columns=({'validatedTasks' : 'grid_number'}))

renamed_df.to_csv("grids_project.csv", index=False, encoding='utf-8')
print("check grids_project.csv file")