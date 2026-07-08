from data_generator.identity import resolve_individual, resolve_household


def test_individual_is_deterministic():
    assert resolve_individual("u-42") == resolve_individual("u-42")


def test_household_groups_individuals():
    h1 = resolve_household("ind_0001")
    h2 = resolve_household("ind_0001")
    assert h1 == h2 and h1.startswith("hh_")


def test_individual_id_format():
    assert resolve_individual("u-7").startswith("ind_")
