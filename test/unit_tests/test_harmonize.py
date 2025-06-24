#!/usr/bin/env python3

def test_strip_version():
    from etl.utils.preprocessing import strip_version
    assert strip_version("ENSG00000139618.15") == "ENSG00000139618"
