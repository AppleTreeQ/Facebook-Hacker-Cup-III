# (c) Evgeny Shavlyugin, 2013
# Regular expressions used for documents parsing

import re;

IdentsParsingRe = '([a-z0-9]+)|([A-Z][a-z0-9]+)|([A-Z][A-Z0-9]+)$|([A-Z][A-Z0-9]+)[^A-Za-z0-9]|([A-Z][A-Z0-9]*)([A-Z][a-z0-9]*)';

def testReParsing():
    print re.findall(IdentsRe, 'QObject');
    print re.findall(IdentsRe, "WM_PAINT" );
    print re.findall(IdentsRe, "wm_paint" );
    print re.findall(IdentsRe, 'glSwapBuffer');
    print re.findall(IdentsRe, 'CMap');
