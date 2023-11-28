import os


import pandas as pd
from sklearn import datasets

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

from evidently.test_suite import TestSuite
from evidently.test_preset import DataDriftTestPreset, DataStabilityTestPreset, DataQualityTestPreset

dir_path = "reports"
data_quality_file_path = "data_drift_suite.html"
data_stability_file_path = "data_drift_suite.html"
data_drift_file_path = "data_drift_suite.html"


@task(name="load_data", retries=3, retry_delay_seconds=20)
def load_bank_data():
    bank_marketing = datasets.fetch_openml(name="bank-marketing", as_frame="auto")
    bank_marketing_data = bank_marketing.frame
    reference = bank_marketing_data[5000:7000]
    prod_simulation_data = bank_marketing_data[7000:]
    batch_size = 2000
    return reference, prod_simulation_data[:batch_size]


@task(name="run_test_suite", retries=3, retry_delay_seconds=20)
def run_data_test_suite(reference: pd.DataFrame, current: pd.DataFrame, test, file_path: str):
    data_drift_suite = TestSuite(tests=[test()])
    data_drift_suite.run(reference_data=reference, current_data=current)

    if not data_drift_suite.as_dict()["summary"]["all_passed"]:
        try:
            os.mkdir(dir_path)
        except OSError:
            print("Error on directory creation")
        data_drift_suite.save_html(os.path.join(dir_path, file_path))


@flow(task_runner=SequentialTaskRunner())
def checks_flow():
    reference, current = load_bank_data()
    run_data_test_suite(reference, current, test=DataQualityTestPreset, file_path=data_quality_file_path)
    run_data_test_suite(reference, current, test=DataStabilityTestPreset, file_path=data_stability_file_path)
    run_data_test_suite(reference, current, test=DataDriftTestPreset, file_path=data_drift_file_path)


checks_flow()
