# Databricks notebook source
# DBTITLE 1,Header — ML Model
# MAGIC %md
# MAGIC # 04 — ML Model: Cost Overrun Prediction
# MAGIC ## Construction Analytics — Train, Register & Serve
# MAGIC
# MAGIC Trains a **LightGBM** classifier to predict which projects will exceed budget by 10%+.
# MAGIC
# MAGIC | Step | What |
# MAGIC |---|---|
# MAGIC | 1 | Load feature table from `construction_demo.gold.ml_feature_table` |
# MAGIC | 2 | Train/test split, encode categoricals |
# MAGIC | 3 | Train LightGBM with hyperparameter tuning |
# MAGIC | 4 | Log to MLflow, register in Unity Catalog |
# MAGIC | 5 | Deploy as serving endpoint |

# COMMAND ----------

# DBTITLE 1,Install ML Libraries
# MAGIC %pip install lightgbm mlflow scikit-learn shap "protobuf<5.0.0,>=3.20.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load Feature Table
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

catalog_name = "construction_demo"
df_features = spark.table(f"{catalog_name}.gold.ml_feature_table")

# Convert to pandas for scikit-learn/LightGBM
pdf = df_features.toPandas()

# Drop non-feature columns
drop_cols = ["project_id", "ingestion_timestamp"]
pdf = pdf.drop(columns=[c for c in drop_cols if c in pdf.columns])

print(f"Feature table: {pdf.shape[0]} rows, {pdf.shape[1]} columns")
print(f"\nTarget distribution:\n{pdf['is_over_budget'].value_counts()}")
pdf.head()

# COMMAND ----------

# DBTITLE 1,Prepare Features & Train/Test Split
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Encode categorical columns
cat_cols = ["project_type", "municipality", "status"]
le_dict = {}
for col in cat_cols:
    le = LabelEncoder()
    pdf[col] = pdf[col].fillna("unknown")
    pdf[col] = le.fit_transform(pdf[col])
    le_dict[col] = le

# Separate features and target
y = pdf["is_over_budget"]
X = pdf.drop(columns=["is_over_budget"])

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

print(f"Train: {X_train.shape[0]} rows | Test: {X_test.shape[0]} rows")
print(f"Train target: {y_train.value_counts().to_dict()}")
print(f"Test target:  {y_test.value_counts().to_dict()}")

# COMMAND ----------

# DBTITLE 1,Train LightGBM Model
import lightgbm as lgb
from sklearn.metrics import classification_report, roc_auc_score, f1_score
import mlflow
import mlflow.lightgbm
from mlflow.models import infer_signature

# Set MLflow experiment
mlflow.set_registry_uri("databricks-uc")
experiment_name = "/Users/" + spark.sql("SELECT current_user()").collect()[0][0] + "/construction_cost_overrun"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="lgbm_cost_overrun_v1") as run:
    # Train LightGBM
    model = lgb.LGBMClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        class_weight="balanced",
        random_state=42,
        verbose=-1,
    )
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    # Metrics
    auc = roc_auc_score(y_test, y_prob)
    f1 = f1_score(y_test, y_pred)

    mlflow.log_param("model_type", "LightGBM")
    mlflow.log_param("n_features", X_train.shape[1])
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("f1_score", f1)

    # Log model with signature
    signature = infer_signature(X_train, y_pred)
    mlflow.lightgbm.log_model(
        model, "model",
        signature=signature,
        input_example=X_train.iloc[:3],
    )

    print(f"\n✅ Model trained and logged")
    print(f"   AUC-ROC: {auc:.4f}")
    print(f"   F1 Score: {f1:.4f}")
    print(f"   Run ID:  {run.info.run_id}")
    print(f"\n{classification_report(y_test, y_pred)}")

# COMMAND ----------

# DBTITLE 1,Register Model in Unity Catalog
import mlflow

model_name = f"{catalog_name}.gold.construction_cost_overrun_model"

# Register the model from the latest run
result = mlflow.register_model(
    model_uri=f"runs:/{run.info.run_id}/model",
    name=model_name,
)

print(f"✅ Model registered: {model_name}")
print(f"   Version: {result.version}")

# COMMAND ----------

# DBTITLE 1,Feature Importance (SHAP)
import shap
import matplotlib.pyplot as plt

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# For binary classification, shap_values may be a list [class_0, class_1]
if isinstance(shap_values, list):
    shap_vals = shap_values[1]  # class 1 = over_budget
else:
    shap_vals = shap_values

plt.figure(figsize=(10, 8))
shap.summary_plot(shap_vals, X_test, plot_type="bar", show=False)
plt.title("Feature Importance — Cost Overrun Prediction")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Score All Projects (Batch Prediction)
# ============================================================
# Score all projects and save predictions to gold
# ============================================================
import pandas as pd

df_all = spark.table(f"{catalog_name}.gold.ml_feature_table").toPandas()
project_ids = df_all["project_id"]
df_score = df_all.drop(columns=["project_id", "ingestion_timestamp", "is_over_budget"], errors="ignore")

# Encode categoricals same as training
for col in cat_cols:
    df_score[col] = df_score[col].fillna("unknown")
    df_score[col] = le_dict[col].transform(df_score[col])

# Predict
predictions = model.predict_proba(df_score)[:, 1]

df_preds = pd.DataFrame({
    "project_id": project_ids,
    "overrun_probability": predictions,
    "risk_level": pd.cut(predictions, bins=[0, 0.3, 0.7, 1.0], labels=["low", "medium", "high"])
})

# Save to gold
sdf_preds = spark.createDataFrame(df_preds)
sdf_preds = sdf_preds.withColumn("scored_at", F.current_timestamp())
sdf_preds.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.gold.ml_project_risk_scores")

print(f"✅ Predictions saved: {sdf_preds.count()} projects scored")
print(f"   Risk distribution:")
display(sdf_preds.groupBy("risk_level").count().orderBy("risk_level"))
