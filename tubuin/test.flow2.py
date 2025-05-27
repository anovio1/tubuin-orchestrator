from prefect import flow, task

@task
def extract():
    print("Extracting data...")
    return {"data": [1, 2, 3]}

@task
def transform(data):
    print(f"Transforming: {data}")
    return [x * 2 for x in data["data"]]

@task
def load(data):
    print(f"Loading: {data}")

@flow
def test_etl_flow():
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

if __name__ == "__main__":
    test_etl_flow()
