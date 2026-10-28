from dataclasses import dataclass

# Wikimedia database names follow fixed conventions (https://meta.wikimedia.org/wiki/Special:SiteMatrix):
# <language><project suffix>, with underscores standing for hyphens in the language code. The entries
# below are the multilingual and special sites that do not follow that pattern.
_SPECIAL = {
    "commonswiki": ("commons.wikimedia.org", "commons"),
    "wikidatawiki": ("www.wikidata.org", "wikidata"),
    "testwikidatawiki": ("test.wikidata.org", "wikidata"),
    "metawiki": ("meta.wikimedia.org", "meta"),
    "specieswiki": ("species.wikimedia.org", "species"),
    "mediawikiwiki": ("www.mediawiki.org", "mediawiki"),
    "wikifunctionswiki": ("www.wikifunctions.org", "wikifunctions"),
    "sourceswiki": ("wikisource.org", "wikisource"),
    "incubatorwiki": ("incubator.wikimedia.org", "incubator"),
    "foundationwiki": ("foundation.wikimedia.org", "foundation"),
    "outreachwiki": ("outreach.wikimedia.org", "outreach"),
    "wikimaniawiki": ("wikimania.wikimedia.org", "wikimania"),
    "loginwiki": ("login.wikimedia.org", "login"),
    "testwiki": ("test.wikipedia.org", "wikipedia"),
    "test2wiki": ("test2.wikipedia.org", "wikipedia"),
    "testcommonswiki": ("test-commons.wikimedia.org", "commons"),
}

_SUFFIXES = (
    ("wiktionary", "wiktionary.org", "wiktionary"),
    ("wikibooks", "wikibooks.org", "wikibooks"),
    ("wikinews", "wikinews.org", "wikinews"),
    ("wikiquote", "wikiquote.org", "wikiquote"),
    ("wikisource", "wikisource.org", "wikisource"),
    ("wikiversity", "wikiversity.org", "wikiversity"),
    ("wikivoyage", "wikivoyage.org", "wikivoyage"),
    ("wikimedia", "wikimedia.org", "wikimedia"),
    ("wiki", "wikipedia.org", "wikipedia"),
)

# Language subdomains whose database name is not the subdomain with hyphens restored.
_SUBDOMAIN_OVERRIDES = {"be_x_old": "be-tarask"}


@dataclass(frozen=True)
class WikiInfo:
    wiki_id: str
    domain: str
    project: str
    language_code: str | None


def describe(wiki_id: str) -> WikiInfo:
    if wiki_id in _SPECIAL:
        domain, project = _SPECIAL[wiki_id]
        return WikiInfo(wiki_id, domain, project, None)
    for suffix, host, project in _SUFFIXES:
        if wiki_id.endswith(suffix) and len(wiki_id) > len(suffix):
            prefix = wiki_id[: -len(suffix)]
            subdomain = _SUBDOMAIN_OVERRIDES.get(prefix, prefix.replace("_", "-"))
            # Chapter wikis (<code>wikimedia) are named after a country or group, not a language.
            language = None if project == "wikimedia" else subdomain
            return WikiInfo(wiki_id, f"{subdomain}.{host}", project, language)
    return WikiInfo(wiki_id, wiki_id, "unknown", None)
