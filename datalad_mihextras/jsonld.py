# run with
# import datalad_mihextras.jsonld as jd; import json; from pathlib import Path
# ds_doc, ds_files = jd.doc2dsverspec(json.load(Path('/tmp/somedoc.jsonld').open()))

from pathlib import Path
from pyld import jsonld
from typing import (
    Dict,
    List,
    Tuple,
)

compaction_context = {
    "afo": "http://purl.allotrope.org/ontologies/result#",
    "dcterms": "https://purl.org/dc/terms/",
    "obo": "https://purl.obolibrary.org/obo/",
    "schema": "https://schema.org"
}

std_type_map = {
    # main types
    'schema:Dataset': 'schema:Dataset',
    'schema:DigitalDocument': 'schema:DigitalDocument',
    'schema:Person': 'schema:Person',
    # alternative/equivalent types mapping onto main/std types
    # ...
}

# keys are recognized terms
# values are their homogenization targets
equivalence_map = dict(std_type_map)
equivalence_map.update({
    # POSIX path
    'afo:AFR_0001928': 'afo:AFR_0001928',
    'dcterms:hasPart': 'dcterms:hasPart',
    'schema:author': 'schema:author',
    'schema:contentSize': 'schema:contentSize',
    'schema:contentUrl': 'schema:contentUrl',
    'schema:email': 'schema:email',
    'schema:name': 'schema:name',
    # md5sum
    'obo:NCIT_C171276': 'obo:NCIT_C171276'
})


def flatten_document(doc: Dict) -> Dict:
    return jsonld.flatten(doc, ctx=compaction_context)


def sort_by_types(objs: List) -> Dict:
    typemap = {}
    for obj in objs:
        # JSON-LD flattening will guarantee a node identifier
        # (could be a blank node ID)
        assert '@id' in obj
        if '@type' not in obj:
            # no type, ignore
            continue
        std_type = std_type_map.get(obj['@type'])
        if not std_type:
            # no recognized type, ignore
            continue
        tm = typemap.get(std_type, {})
        tm[obj['@id']] = obj
        typemap[std_type] = tm
    return typemap


def standardize_obj(obj: Dict, term_map: Dict) -> Dict:
    return {
        # map the key, we know that it is safe or known already
        term_map.get(k, k):
        # map the @type value of an object too
        term_map.get(v, v)
        if k == '@type'
        # recursion into embedded objects (likely @id/@value mappings)
        else standardize_obj(v, term_map)
        if isinstance(v, dict)
        # otherwise leave as is
        else v
        for k, v in obj.items()
        if k in ('@id', '@type', '@value') or k in term_map
    }


#
# the following is specific to the use case of intepreting a doc on a dataset
# (version) to generate a corresponding datalad dataset
#

def _get_fpath(doc: Dict) -> Path:
    name = doc['schema:name']
    if isinstance(name, dict):
        # TODO check for '@type'
        return Path(name['@value'])

    raise ValueError


filespec_term_map = {
    'obo:NCIT_C171276': 'md5sum',
    'schema:contentSize': 'bytesize',
    'schema:contentUrl': 'url',
}


def filedoc2filespec(doc: Dict) -> Tuple:
    path = _get_fpath(doc)
    props = {}
    for term, key in filespec_term_map.items():
        val = doc.get(term)
        if val is not None:
            props[key] = val
    return (path, props)


def doc2dsverspec(doc: Dict) -> Dict:
    flattened = flatten_document(doc)
    obj_by_type = sort_by_types(flattened.get('@graph', []))
    ds_doc = obj_by_type.get('schema:Dataset', {})

    # fish out the document on the dataset, there should be
    # only one
    # TODO later this would need to determine versions,
    # sort them, etc.
    assert len(ds_doc) == 1
    ds_docid, ds_doc = ds_doc.popitem()
    ds_doc = standardize_obj(ds_doc, equivalence_map)

    file_docs = obj_by_type.get('schema:DigitalDocument', {})
    file_docs = {
        fdoc_id:
        standardize_obj(fdoc, equivalence_map)
        for fdoc_id, fdoc in file_docs.items()
    }

    ds_files = {}
    for fdoc in ds_doc.get('dcterms:hasPart', []):
        fdoc_id = fdoc['@id']
        try:
            ds_files.update([
                filedoc2filespec(file_docs[fdoc_id])
            ])
        except Exception as e:
            # TODO log/warn
            print(e)
            continue

    return ds_doc, ds_files
