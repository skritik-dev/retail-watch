import mlflow.xgboost
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import xgboost as xgb
from utils.logger import get_logger

# Config
# We use the 'models:/' URI to get the latest production model dynamically
# For now, we point to the local run artifacts we created yesterday
MODEL_URI = "runs:/e644407300424d03b44de880c4a8cfda/model" 

app = FastAPI(title="RetailWatch Inference Service")

logger = get_logger("API Server")

model = None

class ProductPayload(BaseModel):
    price: float
    price_diff: float
    price_diff_percent: float
    price_rolling_3: float
    price_std_3: float

@app.on_event("startup")
def load_model():
    global model
    logger.info(" [~] Loading model from MLflow.")
    try:
        # Load model as an XGBoost Booster object
        # Note: In a real team, we'd use 'mlflow.pyfunc.load_model' for generic wrapper
        model = mlflow.xgboost.load_model(MODEL_URI)
        logger.info(" [OK] Model loaded successfully.")
    except Exception as e:
        logger.error(f" [x] Failed to load model: {e}")

@app.post("/predict")
def predict_stock_out(payload: ProductPayload):
    if not model:
        logger.error(" [x] Model not loaded.")
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    # 1. Convert API JSON to DataFrame (Expected by XGBoost)
    input_data = pd.DataFrame([payload.model_dump()])
    
    # 2. Convert to DMatrix (required by native XGBoost)
    dmatrix = xgb.DMatrix(input_data)
    
    # 3. Predict
    probability = model.predict(dmatrix)[0] # Returns float 0.0 - 1.0
    
    logger.info(f" [~] Predicted stock out probability: {probability}")
    return {
        "stock_out_probability": float(probability),
        "alert_triggered": bool(probability > 0.5)
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "model_loaded": model is not None}