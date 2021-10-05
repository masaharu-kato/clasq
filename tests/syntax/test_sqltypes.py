"""
    Test SQLTypes
"""
from libsql.syntax import sqltypes as sqt

def test_sqltypes():
    """ Debug """

    vi = sqt.Int(25)
    print(vi)

    vchars = [
        sqt.VarChar[64]('hogefuga'),
        sqt.VarChar[32]('piyofoo'),
        sqt.VarChar[32]('baaaa'),
        sqt.VarChar[64]('efwaw'),
        # VarChar('awfefwea'),
        # VarChar('bbb'),
        sqt.Text[128]('hogefugapiyopiyo'),
        sqt.Text('afefwefjbifjawe32'),
    ]

    for i, vc in enumerate(vchars, 1):
        print(i, type(vc), id(type(vc)), vc.__type_sql__(), id(vc), vc)

