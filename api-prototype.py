import portus
from portus.llm import OpenAI

sample_data = {
    'EmployeeID': [1, 2, 3, 4, 5],
    'Name': ['John', 'Emma', 'Liam', 'Olivia', 'William'],
    'Department': ['HR', 'Sales', 'IT', 'Marketing', 'Finance']
}

# session is serializable, so we may allow users to store/load sessions
session = portus.create_session(llm=OpenAI(temperature=0),
                                autosave=False)  # by default, sessions can be automatically saved

# user can connect dataframe / db connection / local files / etc to the context
session.connect("postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix")
session.connect(sample_data)
session.connect("readme.md")
session.connect(dce)

# asking AI - for now a user can skip thinking about the output format
result = session.ask("What is the revenue for 2024 by month?")

# then a result can be represented in multiple ways
print(result)  # text output
result.df()  # dataframe output
result.plot()  # plot output

# user can explicitly save a session on disk
session.dump("session123.prts")

# Possible SaaS scenarios:
# 1. Automatically save and store all the sessions with artifacts(results) and allow users to access/share them via web UI
# 2. Manage data access for teams, provide default connections for each new session
# 3. Control token consumption
