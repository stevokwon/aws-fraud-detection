## 📌 Fraud Detection Pipeline: Milestone Tracker

We are building a production-grade, AWS-integrated fraud scoring engine. This checklist tracks functionality across 3 critical deployment phases.

---

### ✅ Phase 1: Minimal Viable Batch Pipeline  
**🎯 Goal:** Establish a working fraud prediction engine with S3 I/O

- [ ] Load raw transaction data from S3
- [ ] Apply consistent preprocessing (same as training)
- [ ] Load trained LightGBM model
- [ ] Predict fraud probabilities
- [ ] Apply saved threshold
- [ ] Save output with `fraud_probability`, `predicted_label`
- [ ] Push scored data to S3

---

### ⚙️ Phase 2: Real-World Ready Features  
**🎯 Goal:** Provide metrics, metadata, and traceability for downstream ops and ML teams

- [ ] Calculate & log fraud rate per batch
- [ ] Save batch metadata to S3 (timestamp, fraud %, sample count)
- [ ] Rank & output top-K high-risk transactions
- [ ] Version-controlled model loading (e.g., `model_v1.pkl`)
- [ ] Push metrics to Airflow XCom or logging file
- [ ] Trigger warning if fraud rate > threshold (e.g., 5%)
- [ ] Save only required fields (txn_id, score, prediction) for review team

---

### 🔐 Phase 3: Fintech Production-Grade Features  
**🎯 Goal:** Ensure security, explainability, and auditing in line with fintech industry practices

- [ ] Save SHA256 hash or checksum of model + features for audit
- [ ] Explainability: Add SHAP-lite output (e.g., top feature name per txn)
- [ ] Store scoring logs + errors in S3 or CloudWatch
- [ ] Retry logic for transient failures (e.g., S3 retries)
- [ ] Secure S3 access using IAM roles
- [ ] Encrypt data in transit + at rest (e.g., use KMS-managed buckets)
- [ ] Store DAG & pipeline metadata (e.g., DynamoDB registry or S3 JSON)
- [ ] Monitor drift: Check input schema against training version

---

### 🧪 Optional (Advanced/Bonus Ideas)

- [ ] Fall back to real-time SageMaker or endpoint inference if batch fails
- [ ] Trigger downstream DAG (e.g., review alert, dashboard refresh)
- [ ] Integrate Slack or SES alerting
- [ ] Push top risk scores to fraud investigation UI / dashboard
