"""Tests for expert review agreement (Axis D)."""
import numpy as np
import pandas as pd

from network_idx.validation.expert.review import (
    build_sampling_frame,
    fleiss_kappa,
    run_expert_review,
    score_decile,
)


def test_score_decile_range():
    s = pd.Series(np.arange(100))
    d = score_decile(s)
    assert d.min() == 0
    assert d.max() == 9


def test_build_sampling_frame_covers_cells_and_oversamples():
    rng = np.random.default_rng(1)
    n = 400
    frame = pd.DataFrame(
        {
            "parcel_shape_id": np.arange(n),
            "idx_overall": rng.uniform(0, 100, n),
            "ctx": rng.choice(["urban", "rural"], n),
            "region": rng.choice(["west", "east"], n),
        }
    )
    sample = build_sampling_frame(
        frame, "ctx", "region", per_cell=2, oversample_ids=[7, 11, 13]
    )
    assert {7, 11, 13}.issubset(set(sample["parcel_shape_id"]))
    # no duplicate parcels
    assert sample["parcel_shape_id"].is_unique


def test_fleiss_kappa_perfect_agreement():
    # 3 subjects, 2 categories, all 4 raters agree -> kappa 1
    ratings = np.array([[4, 0], [0, 4], [4, 0]])
    assert abs(fleiss_kappa(ratings) - 1.0) < 1e-9


def test_fleiss_kappa_chance_level_near_zero():
    rng = np.random.default_rng(2)
    # random 3-category ratings by 5 raters over 200 subjects
    rows = []
    for _ in range(200):
        counts = np.bincount(rng.integers(0, 3, 5), minlength=3)
        rows.append(counts)
    k = fleiss_kappa(np.array(rows))
    assert abs(k) < 0.15


def test_run_expert_review_passes_when_experts_track_index():
    rng = np.random.default_rng(3)
    n = 300
    score = pd.Series(rng.uniform(0, 100, n))
    decile = score_decile(score)
    # experts rate 1-5 roughly tracking the decile (halved) plus small noise
    def rater(noise):
        r = np.clip((decile / 2).round() + rng.integers(-noise, noise + 1, n), 0, 5)
        return r.astype(int)
    ratings = pd.DataFrame({"e1": rater(1), "e2": rater(1), "e3": rater(1)})
    rep = run_expert_review(ratings, score, ["e1", "e2", "e3"])
    assert rep.mean_expert_index_kappa > 0.4
    assert rep.passed is True
