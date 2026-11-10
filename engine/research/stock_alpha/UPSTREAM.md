# Upstream notice

`double_ensemble.py` adapts the sample-reweighting and feature-selection algorithm from
Microsoft Qlib's `qlib/contrib/model/double_ensemble.py` under the MIT License. The local version
removes the Qlib `DatasetH` dependency and accepts pandas frames directly while retaining the
six-submodel training sequence, loss-curve sample reweighting, and permutation-based feature
selection.

Upstream source:
<https://github.com/microsoft/qlib/blob/main/qlib/contrib/model/double_ensemble.py>
