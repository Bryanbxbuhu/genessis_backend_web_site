"""
Canonical store chain websites for convenience stores, supermarkets, and pharmacies.
"""

from typing import Optional, Iterable
import re
import unicodedata


STORE_CHAIN_CANONICAL = {
    "ES": {
        "supermarket": {
            "mercadona": "https://www.mercadona.es",
            "dia": "https://www.dia.es",
            "carrefour": "https://www.carrefour.es",
            "alcampo": "https://www.alcampo.es",
            "lidl": "https://www.lidl.es",
            "aldi": "https://www.aldi.es",
            "eroski": "https://www.eroski.es",
            "elcorteingles": "https://www.elcorteingles.es/supercor/",
            "supercor": "https://www.elcorteingles.es/supercor/",
            "hipercor": "https://www.elcorteingles.es/hipercor/",
            "supermercadosmas": "https://www.supermercadosmas.com",
            "bonpreu": "https://www.bonpreuesclat.cat",
            "caprabo": "https://www.caprabo.com",
            "condis": "https://www.condis.es",
            "ahorramas": "https://www.ahorramas.com",
            "masymas": "https://www.masymas.com",
        },
        "convenience": {
            "carrefour": "https://www.carrefour.es",
            "carrefourexpress": "https://www.carrefour.es",
            "carrefourcity": "https://www.carrefour.es",
            "dia": "https://www.dia.es",
            "supercor": "https://www.elcorteingles.es/supercor/",
            "opencor": "https://www.elcorteingles.es/supercor/",
            "alcampo": "https://www.alcampo.es",
            "lidl": "https://www.lidl.es",
            "aldi": "https://www.aldi.es",
        },
        "pharmacy": {
            "farmaciastrebol": "https://farmaciastrebol.com",
            "mifarma": "https://www.mifarma.es",
        },
    },
    "PT": {
        "supermarket": {
            "continente": "https://www.continente.pt",
            "pingodoce": "https://www.pingodoce.pt",
            "lidl": "https://www.lidl.pt",
            "aldi": "https://www.aldi.pt",
            "auchan": "https://www.auchan.pt",
            "intermarche": "https://www.intermarche.pt",
            "minipreco": "https://www.minipreco.pt",
            "spar": "https://www.spar.pt",
        },
        "convenience": {
            "meusuper": "https://www.meusuper.pt",
            "amanhecer": "https://www.amanhecer.pt",
            "continente": "https://www.continente.pt",
            "pingodoce": "https://www.pingodoce.pt",
            "minipreco": "https://www.minipreco.pt",
            "spar": "https://www.spar.pt",
        },
        "pharmacy": {
            "farmaciasportuguesas": "https://www.farmaciasportuguesas.pt",
            "holon": "https://holon.pt",
        },
    },
    "GB": {
        "supermarket": {
            "tesco": "https://www.tesco.com",
            "sainsburys": "https://www.sainsburys.co.uk",
            "asda": "https://www.asda.com",
            "morrisons": "https://www.morrisons.com",
            "waitrose": "https://www.waitrose.com",
            "lidl": "https://www.lidl.co.uk",
            "aldi": "https://www.aldi.co.uk",
            "coop": "https://www.coop.co.uk",
            "iceland": "https://www.iceland.co.uk",
            "marksandspencer": "https://www.marksandspencer.com",
        },
        "convenience": {
            "tesco": "https://www.tesco.com",
            "sainsburys": "https://www.sainsburys.co.uk",
            "coop": "https://www.coop.co.uk",
            "spar": "https://www.spar.co.uk",
            "londis": "https://www.londis.co.uk",
            "onestop": "https://www.onestop.co.uk",
            "nisa": "https://www.nisalocally.co.uk",
            "premier": "https://www.premier-stores.co.uk",
            "costcutter": "https://www.costcutter.co.uk",
            "budgens": "https://www.budgens.co.uk",
        },
        "pharmacy": {
            "boots": "https://www.boots.com",
            "superdrug": "https://www.superdrug.com",
            "lloydspharmacy": "https://www.lloydspharmacy.com",
            "wellpharmacy": "https://www.well.co.uk",
            "rowlands": "https://www.rowlandspharmacy.co.uk",
            "daylewis": "https://www.daylewis.co.uk",
        },
    },
    "FR": {
        "supermarket": {
            "carrefour": "https://www.carrefour.fr",
            "auchan": "https://www.auchan.fr",
            "eleclerc": "https://www.e-leclerc.com",
            "intermarche": "https://www.intermarche.com",
            "casino": "https://www.casino.fr",
            "monoprix": "https://www.monoprix.fr",
            "lidl": "https://www.lidl.fr",
            "aldi": "https://www.aldi.fr",
            "superu": "https://www.magasins-u.com",
        },
        "convenience": {
            "carrefour": "https://www.carrefour.fr",
            "carrefourcity": "https://www.carrefour.fr",
            "carrefourexpress": "https://www.carrefour.fr",
            "monoprix": "https://www.monoprix.fr",
            "monop": "https://www.monoprix.fr",
            "franprix": "https://www.franprix.fr",
            "casino": "https://www.casino.fr",
            "petitcasino": "https://www.casino.fr",
        },
        "pharmacy": {
            "pharmacielafayette": "https://www.pharmacielafayette.com",
            "giphar": "https://www.giphar.fr",
        },
    },
    "CZ": {
        "supermarket": {
            "albert": "https://www.albert.cz",
            "lidl": "https://www.lidl.cz",
            "tesco": "https://www.tesco.cz",
            "kaufland": "https://www.kaufland.cz",
            "billa": "https://www.billa.cz",
            "penny": "https://www.penny.cz",
            "globus": "https://www.globus.cz",
            "coop": "https://www.coop.cz",
        },
        "convenience": {
            "zabka": "https://www.zabka.cz",
            "albert": "https://www.albert.cz",
            "tesco": "https://www.tesco.cz",
            "lidl": "https://www.lidl.cz",
            "coop": "https://www.coop.cz",
        },
        "pharmacy": {
            "drmax": "https://www.drmax.cz",
            "benu": "https://www.benu.cz",
            "pilulka": "https://www.pilulka.cz",
        },
    },
    "US": {
        "supermarket": {
            "walmart": "https://www.walmart.com",
            "target": "https://www.target.com",
            "costco": "https://www.costco.com",
            "kroger": "https://www.kroger.com",
            "safeway": "https://www.safeway.com",
            "albertsons": "https://www.albertsons.com",
            "wholefoods": "https://www.wholefoodsmarket.com",
            "wholefoodsmarket": "https://www.wholefoodsmarket.com",
            "traderjoes": "https://www.traderjoes.com",
            "publix": "https://www.publix.com",
            "wegmans": "https://www.wegmans.com",
            "stopandshop": "https://www.stopandshop.com",
            "shoprite": "https://www.shoprite.com",
            "hmart": "https://www.hmart.com",
            "99ranch": "https://www.99ranch.com",
            "aldi": "https://www.aldi.us",
            "lidl": "https://www.lidl.com",
            "sprouts": "https://www.sprouts.com",
            "meijer": "https://www.meijer.com",
            "keyfood": "https://www.keyfood.com",
            "ctown": "https://ctownsupermarkets.com",
            "presidente": "https://presidentesupermarkets.com",
            "milams": "https://www.milamsmarkets.com",
            "sedanos": "https://sedanos.com",
            "frescoymas": "https://www.frescoymas.com",
            "winndixie": "https://www.winndixie.com",
        },
        "convenience": {
            "7eleven": "https://www.7-eleven.com",
            "seveneleven": "https://www.7-eleven.com",
            "711": "https://www.7-eleven.com",
            "circlek": "https://www.circlek.com",
            "ampm": "https://www.ampm.com",
            "arco": "https://www.arco.com",
            "bp": "https://www.bp.com",
            "wawa": "https://www.wawa.com",
            "speedway": "https://www.speedway.com",
            "caseys": "https://www.caseys.com",
            "bucees": "https://www.buc-ees.com",
            "quiktrip": "https://www.quiktrip.com",
            "kwiktrip": "https://www.kwiktrip.com",
            "sheetz": "https://www.sheetz.com",
            "cumberlandfarms": "https://www.cumberlandfarms.com",
            "holiday": "https://www.holidaystationstores.com",
            "holidaystationstores": "https://www.holidaystationstores.com",
            "loves": "https://www.loves.com",
            "familydollar": "https://www.familydollar.com",
            "dollartree": "https://www.dollartree.com",
            "dollargeneral": "https://www.dollargeneral.com",
            "cvs": "https://www.cvs.com",
            "walgreens": "https://www.walgreens.com",
        },
        "pharmacy": {
            "cvs": "https://www.cvs.com",
            "walgreens": "https://www.walgreens.com",
            "riteaid": "https://www.riteaid.com",
            "duanereade": "https://www.walgreens.com",
            "capsule": "https://capsule.com",
        },
    },
    "JP": {
        "supermarket": {
            "aeon": "https://www.aeon.info",
            "itoyokado": "https://www.itoyokado.co.jp",
            "seiyu": "https://www.seiyu.co.jp",
            "life": "https://www.lifecorp.jp",
            "maruetsu": "https://www.maruetsu.co.jp",
            "daiei": "https://www.daiei.co.jp",
            "gyomu": "https://www.gyomusuper.jp",
            "okstore": "https://ok-corporation.jp",
        },
        "convenience": {
            "7eleven": "https://www.sej.co.jp",
            "seveneleven": "https://www.sej.co.jp",
            "711": "https://www.sej.co.jp",
            "familymart": "https://www.family.co.jp",
            "lawson": "https://www.lawson.co.jp",
            "ministop": "https://www.ministop.co.jp",
            "seicomart": "https://www.seicomart.co.jp",
            "ampm": "https://www.family.co.jp",
            "circlek": "https://www.family.co.jp",
            "sunkus": "https://www.family.co.jp",
        },
        "pharmacy": {
            "matsumotokiyoshi": "https://www.matsukiyo.co.jp",
            "matsukiyo": "https://www.matsukiyo.co.jp",
            "welcia": "https://www.welcia-yakkyoku.co.jp",
            "sugidrug": "https://www.drug-sugi.co.jp",
            "sugi": "https://www.drug-sugi.co.jp",
            "tsuruha": "https://www.tsuruha.co.jp",
            "sundrug": "https://www.sundrug.co.jp",
        },
    },
    "RU": {
        "supermarket": {
            "pyaterochka": "https://5ka.ru",
            "perekrestok": "https://www.perekrestok.ru",
            "magnit": "https://magnit.ru",
            "lenta": "https://lenta.com",
            "auchan": "https://www.auchan.ru",
            "metro": "https://www.metro-cc.ru",
            "dixy": "https://www.dixy.ru",
            "vkusvill": "https://vkusvill.ru",
            "spar": "https://myspar.ru",
        },
        "convenience": {
            "vkusvill": "https://vkusvill.ru",
            "fixprice": "https://fix-price.com",
            "pyaterochka": "https://5ka.ru",
            "magnit": "https://magnit.ru",
            "perekrestok": "https://www.perekrestok.ru",
        },
        "pharmacy": {
            "apteka366": "https://apteka366.ru",
            "366": "https://apteka366.ru",
            "rigla": "https://www.rigla.ru",
            "gorzdrav": "https://gorzdrav.org",
            "aptekaru": "https://apteka.ru",
            "eapteka": "https://eapteka.ru",
        },
    },
}


SHORT_JOINED_TOKENS = {
    "coop",
}


def _normalize_text(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _name_parts(name: str) -> tuple[str, list[str], str]:
    text = _normalize_text(name)
    words = text.split() if text else []
    joined = "".join(words)
    return text, words, joined


def _token_matches(token: str, words: list[str], joined: str) -> bool:
    if token in words:
        return True
    if not joined:
        return False
    if any(ch.isdigit() for ch in token):
        return token in joined
    if len(token) >= 6:
        return token in joined
    if token in SHORT_JOINED_TOKENS:
        return token in joined
    return False


def _iter_chain_maps(
    country_code: Optional[str],
    category: Optional[str],
) -> Iterable[dict[str, str]]:
    if country_code:
        country = STORE_CHAIN_CANONICAL.get(country_code.strip().upper(), {})
        if category:
            mapping = country.get(category, {})
            if mapping:
                yield mapping
            return
        yield from country.values()
        return
    for country in STORE_CHAIN_CANONICAL.values():
        if category:
            mapping = country.get(category, {})
            if mapping:
                yield mapping
            continue
        yield from country.values()


def detect_store_chain(
    name: str,
    country_code: Optional[str] = None,
    category: Optional[str] = None,
) -> Optional[str]:
    """Detect a store chain key from a name string."""
    _, words, joined = _name_parts(name)
    if not words and not joined:
        return None
    for mapping in _iter_chain_maps(country_code, category):
        for token in sorted(mapping.keys(), key=len, reverse=True):
            if _token_matches(token, words, joined):
                return token
    return None


def is_store_chain_name(
    name: str,
    country_code: Optional[str] = None,
    category: Optional[str] = None,
) -> bool:
    """Return True if the name looks like a known chain for that country."""
    return detect_store_chain(name, country_code, category) is not None


def canonicalize_store_website(
    name: str,
    country_code: Optional[str],
    website: Optional[str] = None,
    *,
    category: Optional[str] = None,
) -> Optional[str]:
    """Return a canonical store chain website if detected, otherwise keep existing."""
    country = (country_code or "").strip().upper()
    if not country:
        return website
    token = detect_store_chain(name, country, category)
    if not token:
        return website
    mapping = STORE_CHAIN_CANONICAL.get(country, {})
    if category:
        canonical = mapping.get(category, {}).get(token)
        return canonical or website
    for category_map in mapping.values():
        canonical = category_map.get(token)
        if canonical:
            return canonical
    return website
