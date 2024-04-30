import os
import pickle
import nbformat
import jupytext
from nbconvert.preprocessors import ExecutePreprocessor
from github import Github, InputFileContent

from dagster import asset

@asset(
    group_name='visualization'
)
def monthly_earthquakes_notebook(earthquakes_by_month_and_magnitude):
    markdown = f"""
# Earthquakes

```python
import pickle
earthquakes_by_month_and_magnitude = pickle.loads({pickle.dumps(earthquakes_by_month_and_magnitude)!r})
```

## Earthquakes by Month and Magnitude
```python
import plotly.express as px
fig = px.bar(earthquakes_by_month_and_magnitude, x='month_date', y='count', color='magnitude')
fig.show('png')
```
    """
    nb = jupytext.reads(markdown, "md")
    ExecutePreprocessor().preprocess(nb)
    return nbformat.writes(nb)


@asset(
    group_name='visualization'
)
def monthly_earthquakes_notebook_gist(context, monthly_earthquakes_notebook):
    github_api = Github(os.getenv('GITHUB_ACCESS_TOKEN'))
    auth_user = github_api.get_user()
    gist = auth_user.create_gist(
        public=False,
        files={
            "monthly_earthquakes_notebook.ipynb": InputFileContent(monthly_earthquakes_notebook),
        },
    )
    context.log.info(f"Notebook created at {gist.html_url}")
    return gist.html_url
