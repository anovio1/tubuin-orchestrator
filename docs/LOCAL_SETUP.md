## Installation

### 1. Set up venv

- Note: Below is using powershell because WSL was leaking memory over time

  ```bash
  python -m venv .venv
  .venv\Scripts\activate
  pip install --upgrade pip
  ```

### 2. Install dependencies

- Note: You can also do a clean install, call `pip install prefect` instead

  `pip install -r requirements.txt -c requirements.lock.txt`

### 3. Run the Local Prefect Server

1.  Start Docker Desktop

    ```bash
    cd prefect-server
    docker compose up
    ```

2.  Open another powershell

    ```bash
    prefect config set PREFECT_API_URL=http://localhost:4200/api
    prefect config view
    ```

Visit http://localhost:4200 to see a basic dashboard of flows, tasks, and runs.

- ### _Misc_
  In another terminal tab, run:  
   `prefect dev agent`  
   This enables Prefect to schedule and trigger flows later (even locally).

## Post-Install

- New Project?

1. Create a project folder

   ```
   cd {project_folder}
   prefect init
   ```

2. Create a py file with flow
   ```
   prefect deploy
   ```

## Use

1. Create a flow (Follow Prefect docs here)
2. In terminal:
   ```bash
   prefect deploy   # this will ask you to create a pool if one does not exist
   prefect worker start --pool "#Poolname#" # use the pool you created
   ```

## Utils

### Cancel all late runs

```
cd {projectfolder}
python3 utils/cancel_all_late_runs.py
```

## Notes

### Write out requirements (light)

`pip list --not-required --format=freeze > requirements.txt`

### Then lock, subdependencies

`pip freeze > requirements.lock`
