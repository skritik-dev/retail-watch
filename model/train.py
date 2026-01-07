import pandas as pd
import xgboost as xgb
from sklearn.metrics import precision_score, recall_score, f1_score
import mlflow
import mlflow.xgboost
from pathlib import Path
from utils.logger import get_logger

logger = get_logger("Training")

BASE_DIR = Path(__file__).resolve().parent.parent  

# Config
INPUT_FILE = BASE_DIR / "datasets/training_data.csv"
EXPERIMENT_NAME = "retail_stock_prediction"

def train_model():
    logger.info(" [~] Starting training pipeline.")
    
    # Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        logger.error(" [!] Data not found.")
        return

    if df.empty:
        logger.error(" [!] Dataset is empty. Need more history to train.")
        return

    #! Very critical step: can't use train-test split, because I'm working with time series data.
    #? We cannot shuffle. We take the first 80% as train, last 20% as test.
    train_size = int(len(df) * 0.8)
    train_df = df.iloc[:train_size]
    test_df = df.iloc[train_size:]
    
    X_train = train_df.drop('target_stock_out', axis=1)
    y_train = train_df['target_stock_out']
    
    X_test = test_df.drop('target_stock_out', axis=1)
    y_test = test_df['target_stock_out']

    # Setup MLflow
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    with mlflow.start_run():
        # Hyperparameters
        params = {
            "max_depth": 3,
            "eta": 0.1,
            "objective": "binary:logistic",
            "eval_metric": "logloss"
        }
        
        mlflow.log_params(params)
        
        # Train XGBoost
        #? Convert to DMatrix (XGBoost optimized format)
        dtrain = xgb.DMatrix(X_train, label=y_train)
        dtest = xgb.DMatrix(X_test, label=y_test)
        
        model = xgb.train(
            params, 
            dtrain, 
            num_boost_round=100
        )
        
        # Evaluate
        y_probs = model.predict(dtest)
        y_pred = [1 if p > 0.5 else 0 for p in y_probs]
        
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        
        logger.info(f" [OK] Results - Precision: {precision:.2f}, Recall: {recall:.2f}, F1: {f1:.2f}")
        
        # Log Metrics
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1", f1)
        
        # Save Model Artifact
        mlflow.xgboost.log_model(model, "model")
        
        logger.info(" [OK] Model trained and logged to MLflow.")

if __name__ == "__main__":
    train_model()