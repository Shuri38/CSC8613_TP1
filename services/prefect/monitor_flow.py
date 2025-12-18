import os
from pathlib import Path
from pprint import pprint

import pandas as pd
from prefect import flow, task

from evidently import Report
from evidently.presets import DataDriftPreset, DataSummaryPreset
from evidently.metrics import ValueDrift
from evidently import Dataset
from evidently import DataDefinition

# ----------------------------
# Configuration
# ----------------------------
REPORT_DIR = os.getenv("REPORT_DIR", "/reports/evidently")
DATA_DIR = os.getenv("DATA_DIR", "/data/seeds")  # dossier contenant month_000 et month_001

# Dates associées aux dossiers month_000 et month_001
AS_OF_REF_DEFAULT = "month_000"
AS_OF_CUR_DEFAULT = "month_001"

# ----------------------------
# Chargement CSV (données locales)
# ----------------------------
def load_csv_data(as_of: str) -> pd.DataFrame:
    """
    Construit le dataset final (features + label si disponible) à partir des CSV.
    """
    base_path = Path(DATA_DIR) / as_of

    users = pd.read_csv(base_path / "users.csv")
    usage = pd.read_csv(base_path / "usage_agg_30d.csv")
    payments = pd.read_csv(base_path / "payments_agg_90d.csv")
    support = pd.read_csv(base_path / "support_agg_90d.csv")
    subscriptions = pd.read_csv(base_path / "subscriptions.csv")

    # Merge sur user_id
    df = users.merge(usage, on="user_id", how="left")
    df = df.merge(payments, on="user_id", how="left")
    df = df.merge(support, on="user_id", how="left")
    df = df.merge(subscriptions, on="user_id", how="left")

    # Labels si présentes
    labels_path = base_path / "labels.csv"
    if labels_path.exists():
        labels = pd.read_csv(labels_path)
        df = df.merge(labels, on="user_id", how="left")

    return df

# ----------------------------
# Evidently dataset wrapper
# ----------------------------
def build_dataset_from_df(df: pd.DataFrame) -> Dataset:
    ignored = ["user_id"]
    cat_cols = [c for c in df.columns if df[c].dtype in ["object", "bool"] and c not in ignored]
    num_cols = [c for c in df.columns if c not in cat_cols + ignored]

    definition = DataDefinition(
        numerical_columns=num_cols,
        categorical_columns=cat_cols,
    )
    return Dataset.from_pandas(df, data_definition=definition)

# ----------------------------
# Prefect tasks
# ----------------------------
@task
def build_dataset(as_of: str) -> pd.DataFrame:
    return load_csv_data(as_of)

@task
def compute_target_drift(reference_df: pd.DataFrame, current_df: pd.DataFrame) -> float:
    if "churn_label" not in reference_df.columns or "churn_label" not in current_df.columns:
        print("[Target drift] churn_label absent -> target drift non calculé")
        return float("nan")

    if reference_df["churn_label"].dropna().empty or current_df["churn_label"].dropna().empty:
        print("[Target drift] labels vides -> target drift non calculé")
        return float("nan")

    ref_rate = float(reference_df["churn_label"].astype(int).mean())
    cur_rate = float(current_df["churn_label"].astype(int).mean())
    target_drift = abs(cur_rate - ref_rate)
    print(f"[Target drift] ref_rate={ref_rate:.4f} cur_rate={cur_rate:.4f} abs_diff={target_drift:.4f}")
    return target_drift

@task
def run_evidently(reference_df: pd.DataFrame, current_df: pd.DataFrame, as_of_ref: str, as_of_cur: str):
    Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)
    DRIFT_SHARE_THRESHOLD = 0.3  # seuil arbitraire

    metrics = [
        DataSummaryPreset(),
        DataDriftPreset(drift_share=DRIFT_SHARE_THRESHOLD),
    ]
    if "churn_label" in reference_df.columns:
        metrics.append(ValueDrift(column="churn_label"))

    report = Report(metrics=metrics)
    eval_result = report.run(
        reference_data=build_dataset_from_df(reference_df),
        current_data=build_dataset_from_df(current_df),
    )

    html_path = Path(REPORT_DIR) / f"drift_{as_of_ref}_vs_{as_of_cur}.html"
    json_path = Path(REPORT_DIR) / f"drift_{as_of_ref}_vs_{as_of_cur}.json"
    eval_result.save_html(str(html_path))
    eval_result.save_json(str(json_path))

    summary = eval_result.dict()
    pprint(summary)

    drift_share = None
    for metric in summary.get("metrics", []):
        if "DriftedColumnsCount" in metric.get("metric_id", ""):
            drift_share = metric["value"]["share"]

    if drift_share is None:
        drift_share = 0.0

    return {
        "html": str(html_path),
        "json": str(json_path),
        "drift_share": float(drift_share),
    }

@task
def decide_action(as_of_ref: str, as_of_cur: str, drift_share: float, target_drift: float, threshold: float = 0.3) -> str:
    if drift_share >= threshold:
        return (
            f"RETRAINING_TRIGGERED (SIMULÉ) drift_share={drift_share:.2f} >= {threshold:.2f} "
            f"(target_drift={target_drift if target_drift == target_drift else 'NaN'})"
        )
    return (
        f"NO_ACTION drift_share={drift_share:.2f} < {threshold:.2f} "
        f"(target_drift={target_drift if target_drift == target_drift else 'NaN'})"
    )

# ----------------------------
# Prefect flow
# ----------------------------
@flow(name="monitor_month")
def monitor_month_flow(
    as_of_ref: str = AS_OF_REF_DEFAULT,
    as_of_cur: str = AS_OF_CUR_DEFAULT,
    threshold: float = 0.3,
):
    ref_df = build_dataset(as_of_ref)
    cur_df = build_dataset(as_of_cur)

    tdrift = compute_target_drift(ref_df, cur_df)
    res = run_evidently(ref_df, cur_df, as_of_ref, as_of_cur)
    msg = decide_action(as_of_ref, as_of_cur, res["drift_share"], tdrift, threshold)

    print(
        f"[Evidently] report_html={res['html']} report_json={res['json']} "
        f"drift_share={res['drift_share']:.2f} -> {msg}"
    )

if __name__ == "__main__":
    monitor_month_flow()
