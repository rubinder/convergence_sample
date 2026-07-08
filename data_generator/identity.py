import hashlib


def _h(value: str, mod: int) -> int:
    return int(hashlib.sha256(value.encode()).hexdigest(), 16) % mod


def resolve_individual(seed: str) -> str:
    return f"ind_{_h(seed, 5000):04d}"


def resolve_household(individual_id: str) -> str:
    # ~2.5 individuals per household
    return f"hh_{_h(individual_id, 2000):04d}"
