from codegraph.resolvers.scip import scip_pb2


def test_roundtrip_index_with_occurrence():
    idx = scip_pb2.Index()
    doc = idx.documents.add()
    doc.relative_path = "app/main.py"
    doc.position_encoding = scip_pb2.PositionEncoding.UTF8CodeUnitOffsetFromLineStart
    occ = doc.occurrences.add()
    occ.symbol = "scip-python python demo 0.1 `app.main`/handler()."
    occ.range.extend([3, 4, 3, 11])
    occ.symbol_roles = scip_pb2.SymbolRole.Definition

    data = idx.SerializeToString()
    parsed = scip_pb2.Index()
    parsed.ParseFromString(data)

    d = parsed.documents[0]
    assert d.relative_path == "app/main.py"
    assert list(d.occurrences[0].range) == [3, 4, 3, 11]
    assert d.occurrences[0].symbol_roles & scip_pb2.SymbolRole.Definition


def test_symbol_roles_bitmask_values():
    assert scip_pb2.SymbolRole.Definition == 1
    assert scip_pb2.SymbolRole.Import == 2
