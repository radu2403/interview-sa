from dataclasses import dataclass


@dataclass(frozen=True)
class MarvelConst:
    page_id = "page_id"
    name = "name"
    urls_lug = "urlslug"
    id = "id"
    eye = "eye"
    hair = "hair"
    sex = "sex"
    gsm = "gsm"
    alive = "alive"
    appearances = "appearances"
    first_appearance = "first appearance"
    year = "year"