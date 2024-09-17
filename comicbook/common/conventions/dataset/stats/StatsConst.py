from dataclasses import dataclass


@dataclass(frozen=True)
class StatsConst:
    id = "ID"
    name = "Name"
    alignment = "Alignment"
    gender = "Gender"
    eye_color = "EyeColor"
    race = "Race"
    hair_color = "HairColor"
    publisher = "Publisher"
    skin_color = "SkinColor"
    height = "Height"
    weight = "Weight"


