from typing import List
import numpy as np

def labels_from_bins(bins: np.ndarray) -> List[str]:
    """
    Build readable class labels from ascending bin edges (exclusive left, inclusive right).
    For bins = [b1, b2, ..., bk], labels are:
      '≤ b1', '(b1, b2]', '(b2, b3]', ..., '(b_{k-1}, b_k]'
    """
    labels = [f"≤ {bins[0]:.2f}"]
    for a, b in zip(bins[:-1], bins[1:]):
        labels.append(f"({a:.2f}, {b:.2f}]")
    return labels


def visibility_mask(num_vars: int, num_years: int, var_idx: int, year_idx: int) -> List[bool]:
    """Helper: visibility vector that shows only (var_idx, year_idx)."""
    mask = [False] * (num_vars * num_years)
    mask[var_idx * num_years + year_idx] = True
    return mask