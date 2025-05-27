# Have a project?
## create a visual flow /DAG
cd folder
prefect deploy
prefect worker start # pool will be created after the previous, get the name of it

# New project?
## create a visual flow /DAG
create a project folder
cd folder
prefect init
create a py file with flow
prefect deploy

# Commands
##   Run the Local UI 
open new powershell
cd prefect-server
Start Docker Desktop
docker compose up

open another powershell
prefect config set PREFECT_API_URL=http://localhost:4200/api
prefect config view

Visit http://localhost:4200 to see a basic dashboard of flows, tasks, and runs.

🔁 In another terminal tab, run:
prefect dev agent
This enables Prefect to schedule and trigger flows later (even locally).


# Set up a project
make directory
make flow.py
write etl flow

# Installation
## set up venv, activate
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip

pip install prefect

# or use requirements
pip install -r requirements.txt -c requirements.lock.txt


# Write out requirements (light), then clean it up so just your manually installed packages
pip list --not-required --format=freeze > requirements.txt

# Then lock, subdependencies
pip freeze > requirements.lock
