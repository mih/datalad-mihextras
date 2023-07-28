
from __future__ import annotations

from pyld import jsonld
from typing import Dict


def homogenize(
    doc: Dict,
    term_map: Dict,
    ctx: Dict | None = None,
) -> Dict:
    """Homogenize a JSON-LD document to a target terminology.

    The document is

    1. converted to RDF triples
    2. the given term mapping is applied to any IRIs and data type
       declarations
    3. converted back from triples to a compacted JSON-LD document

    Parameters
    ----------
    doc: dict
      JSON-LD document to homogenize
    term_map: dict
      A mapping of IRIs of terms to their individual homogenization target
      IRIs. Any IRIs, source or target must be long-form, i.e., not compact
      or relative.
    ctx: dict, optional
      If given, defines the compaction context.
    """
    rdf = jsonld.to_rdf(
        doc,
        dict(produceGeneralizedRdf=False),
    )
    assert list(rdf.keys()) == ['@default']
    triples = rdf['@default']
    triples = [
        _homogenize_triple(triple, term_map) for triple in triples
    ]

    hdoc = jsonld.from_rdf(
        {'@default': triples},
        dict(useNativeTypes=True),
    )

    return jsonld.compact(hdoc, ctx or {})


def _homogenize_triple(triple: Dict, term_map: Dict) -> Dict:
    return {
        ename: _homogenize_entity(espec, term_map)
        # iterate over subject, predicate, object
        for ename, espec in triple.items()
    }


def _homogenize_entity(entity: Dict, term_map: Dict) -> Dict:
    t = entity['type']
    if t == 'IRI':
        v = entity['value']
        entity['value'] = term_map.get(v, v)
    elif t == 'literal':
        d = entity['datatype']
        entity['datatype'] = term_map.get(d, d)
    # we return (although modified in-place) to be able to used
    # inside a comprehension construct
    return entity


# this context is a convenience
compaction_context = {
    "afo": "http://purl.allotrope.org/ontologies/result#",
    "dcterms": "https://purl.org/dc/terms/",
    "nfo": "https://www.semanticdesktop.org/ontologies/2007/03/22/nfo/#",
    "obo": "https://purl.obolibrary.org/obo/",
    "schema": "https://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}


# keys are recognized terms given by full IRI
# values are their homogenization targets (again full IRI)
equivalence_map = {
    'http://purl.obolibrary.org/obo/OGI.owl#Author':
    'https://schema.org/author',
    'https://dbpedia.org/ontology/fileSize':
    'https://www.semanticdesktop.org/ontologies/2007/03/22/nfo/#fileSize',
}


dsfiles_frame = {
    '@context': {
        "schema": "https://schema.org/",
        "md5sum": "https://purl.obolibrary.org/obo/NCIT_C171276",
        "url": "schema:contentUrl",
        "hasPart": "https://purl.org/dc/terms/hasPart",
        "name": "schema:name",
        'posixpath': 'http://purl.allotrope.org/ontologies/result#AFR_0001928',
        'bytesize': 'https://www.semanticdesktop.org/ontologies/2007/03/22/nfo/#fileSize',
    },
    '@type': 'schema:Dataset',
    'hasPart': {
        '@type': 'schema:DigitalDocument',
    }
}



