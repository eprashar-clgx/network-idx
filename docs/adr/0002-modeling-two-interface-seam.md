# Modeling exposes two interfaces; notebooks are thin drivers

`modeling` exposes two interfaces: `train(training_frame) -> model_artifacts`
(cluster → classify → SHAP) and `fit_scoring_rules(training_frame, shap) ->
{weights, scaling_params}`. Notebooks hold **no** product logic — they only load the
training frame, call these interfaces, and visualize. We split into two interfaces
because the model re-run happens **far less frequently** than a feature/data refresh:
`train` reruns on a model refresh (roughly annual), `fit_scoring_rules` reruns whenever
the feature population changes (roughly monthly). Cadences are approximate and may
change; the load-bearing decision is that product logic lives in tested `src`, not in
notebooks.
