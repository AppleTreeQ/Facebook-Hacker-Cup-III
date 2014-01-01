# (c) Evgeny Shavlyugin, 2013
# Test for regular expresions

import re;

IdentsParsingRe = '([a-z0-9]+)|([A-Z][a-z0-9]+)|([A-Z][A-Z0-9]+)$|([A-Z][A-Z0-9]+)[^A-Za-z0-9]|([A-Z][A-Z0-9]*)([A-Z][a-z0-9]*)';

print re.findall(identsRe, 'QObject');
print re.findall(identsRe, "WM_PAINT" );
print re.findall(identsRe, "wm_paint" );
print re.findall(identsRe, 'glSwapBuffer');
print re.findall(identsRe, 'CMap');
