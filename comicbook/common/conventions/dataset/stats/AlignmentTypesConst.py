from dataclasses import dataclass


@dataclass(frozen=True)
class AlignmentTypesConst:
    bad = "bad"
    good = "good"