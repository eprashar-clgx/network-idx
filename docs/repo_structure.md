flowchart LR
  A[extraction<br/>SQL feature tables] --> B[04_01 feature_engg<br/>growth + telecom]
  B --> C[04_02 correlations]
  C --> D[05 clustering<br/>StandardScaler + KMeans]
  D --> E[05 classification<br/>lightgbm / rforest + SHAP]
  B -. same fills/caps .-> F[src scaling_params<br/>+ feature tables]