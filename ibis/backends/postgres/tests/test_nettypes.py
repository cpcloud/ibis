""" Tests for macaddr and inet data types"""

import ibis


def test_macaddr(alltypes):
    macaddr_value = '00:0a:95:9d:68:16'
    lit = ibis.literal(macaddr_value, type='macaddr').name('tmp')
    expr = alltypes[[alltypes.id, lit]].head(1)
    df = expr.execute()
    assert df['tmp'].iloc[0] == macaddr_value


def test_inet(alltypes):
    inet_value = '00:0a:95:9d:68:16'
    lit = ibis.literal(inet_value, type='inet').name('tmp')
    expr = alltypes[[alltypes.id, lit]].head(1)
    df = expr.execute()
    assert df['tmp'].iloc[0] == inet_value
