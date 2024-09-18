from dataclasses import dataclass


@dataclass(frozen=True)
class DcConst:
    page_id = "page_id"
    name = "name"
    urls_lug = "urlslug"
    id = "ID"
    eye = "EYE"
    hair = "HAIR"
    sex = "SEX"
    gsm = "GSM"
    alive = "ALIVE"
    appearances = "APPEARANCES"
    first_appearance = "FIRST APPEARANCE"
    year = "YEAR"