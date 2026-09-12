from durable_worker_example.domain.features import extract_features, score_features


def build_features(text: str) -> dict[str, int]:
    return extract_features(text)


def score_feature_pairs(query: dict[str, int], candidates: list[tuple[int, dict[str, int]]]) -> list[tuple[int, float]]:
    return [(artifact_id, score_features(query, features)) for artifact_id, features in candidates]
