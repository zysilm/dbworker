import math
import re
from collections import Counter


def extract_features(text: str) -> dict[str, int]:
    """CPU task: turn text into a compact token-frequency feature map."""
    return dict(Counter(re.findall(r"[a-z0-9]+", text.lower())))


def score_features(query: dict[str, int], candidate: dict[str, int]) -> float:
    """CPU task: cosine similarity for two feature maps."""
    numerator = sum(value * candidate.get(key, 0) for key, value in query.items())
    query_norm = math.sqrt(sum(value * value for value in query.values()))
    candidate_norm = math.sqrt(sum(value * value for value in candidate.values()))
    return numerator / (query_norm * candidate_norm) if query_norm and candidate_norm else 0.0
