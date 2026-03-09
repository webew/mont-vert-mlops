# test_evidently.py
import pandas as pd
import numpy as np
from evidently import Dataset, DataDefinition
from evidently.presets import DataDriftPreset
from evidently import Report

df_ref = pd.DataFrame({
    "nb_patients": np.random.normal(150, 10, 365),
    "nb_repas": np.random.normal(400, 20, 365),
})
df_cur = pd.DataFrame({
    "nb_patients": np.random.normal(250, 10, 365),  # drift volontaire
    "nb_repas": np.random.normal(400, 20, 365),
})

definition = DataDefinition(numerical_columns=["nb_patients", "nb_repas"])
ref = Dataset.from_pandas(df_ref, data_definition=definition)
cur = Dataset.from_pandas(df_cur, data_definition=definition)

report = Report(metrics=[DataDriftPreset()])
result = report.run(reference_data=ref, current_data=cur)

import json
print(json.dumps(result.dict(), indent=2, default=str)[:3000])