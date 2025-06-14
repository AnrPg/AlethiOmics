#!/usr/bin/env python3

from etl.harmonise import enrich_ontology_term

print(enrich_ontology_term("CL:0000057"))
print("\n")
print(enrich_ontology_term("HsapDv:0000087"))
print("\n")
print(enrich_ontology_term("http://purl.obolibrary.org/obo/MONDO_0005148"))
print("\n")

