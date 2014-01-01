# (c) Evgeny Shavlyugin, 2013
# Class for extracting features from documents.
# Returns an unique string representation for each extracted feature.

from HTMLParser import HTMLParser;
from htmlentitydefs import name2codepoint;
import nltk;
import re;
import ParsingRegexps;
from collections import Counter;

#######################################################################
### HTMLParser for StackExchange data format ##########################
#######################################################################
# Auxillary class for parsing html
# For a current implementation processes tags <code>, <p>
class ExtractorHTMLParser(HTMLParser):
	_currentTag = "";
	_currentParagraph = "";
	_currentCode = "";
	_featureExtractor = None;

	def __init__(self, featureExtractor):
		self._featureExtractor = featureExtractor;
		HTMLParser.__init__(self);

	def handle_starttag(self, tag, attrs):
		if tag == "code":
			self._currentCode = "";
			self._currentTag = "code";
		elif tag == "p":
			self._currentParagraph = "";
			self._currentTag = "p";

	def handle_endtag(self, tag):
		if tag == "code":
			self._currentParagraph = self._currentParagraph + " CODE_SNIPPET ";
			self._featureExtractor.onCodeExtracted(self._currentCode);
			self._currentCode = "";
			self._currentTag = "p";
		elif tag == "p":
			self._featureExtractor.onParagraphExtracted(self._currentParagraph);
			self._currentParagraph = "";
			self._currentTag = "";
 
	def handle_data(self, data):
		if self._currentTag == "p":
			self._currentParagraph = self._currentParagraph + data;
		elif self._currentTag == "code":
			self._currentCode = self._currentCode + data;

        def handle_entityref(self, data):
                try:
                        c = chr(name2codepoint[data])
                except:
                        c = data + ' ';
                self.handle_data(c);

        def getCharRangeIdentifier(self, c):
                try:
                        i = int(c);
                except:
                        return c;
                if i >= 32 and i < 128:
                        return chr(i);
                if i >= 0x590 and i <= 0x5ff:
                        return 'hebrewcharacter';
                elif i >= 0x600 and i <= 0x6ff:
                        return 'arabiccharacter';
                elif i >= 0x2300 and i <= 0x23ff:
                        return "techcharacter";
                elif i >= 0x4e00 and i <= 0x9fff:
                        return "cjkcharacter";
                else:
                        print "Warning: unknown character with code ", i;
                        return 'unknownunicode';

        def handle_charref(self, data):
                if data.startswith('x'):
                        c = data[1:];
                else:
                        c = data;
                c = self.getCharRangeIdentifier(data) + ' ';
                self.handle_data(c);
#######################################################################
### FeatureExtractor itself ###########################################
#######################################################################


class FeatureExtractor:
        _features = Counter();
        _lemmatizer = None;
        _debug = False;

        def __init__(self, debug):
                self._debug = debug;

	# for autotest of private methods
        def testMyself(self):
                print 'True=', self._isIdentifier('fdklas');
                print 'False=', self._isIdentifier('..,.,');

	# a main method for feature extraction. Returns the result in form
	# [(f, count), ...] where f is a string representation of feature
	# and count is number of occurences of a feature in document
	def processText(self, id, heading, html):
                self._lemmatizer = nltk.stem.Porter();
                self._features = Counter();
                self._extractFromHeader(heading);
		htmlParser = ExtractorHTMLParser(self);
                try:
                        htmlParser.feed( html );
                except:
                        print "Ooops! Error has occured";
                        print id;
                        print html;
                        return [];
                return self._features.most_common();

        def onParagraphExtracted(self, text):
                if self._debug:
                        print ("Paragraph extracted: %s" % text);
                self._extractFromContent(text);

	def onCodeExtracted(self, text):
                if self._debug:
                        print ("Code extracted: %s" % text);
                self._extractFromCode(text);

        def _isNumber(self, str):
                try:
                        float(str);
                except ValueError:
                        return False;
                return True;

        def _isIdentifier(self, str):
                return re.search('(\w+)', str) != None;

        def _splitText(self, text):
                res = re.findall(r'(\w+)|([^\w])', text);
                if not res:
                        return [];
                res = reduce( lambda x, y: x + y, 
                              map( lambda x: list(x), res ) );
                res = map( lambda x: x.strip(), res);
                res = filter( lambda x: len(x) > 0, res);
                return res;

        def _normalizeWords(self, text):
		return [word.lower() for word in text];

        def _genNGrams(self, name, features, index, maxCount, canReplaceIdent):
                res = [];
                idCount = 0;
                for i in range(1,maxCount):
                        if index + i <= len(features):
                                if features[i+index-1][1] == 'i':
                                        idCount = idCount + 1;
                                if idCount > 2:
                                        break;
                                
                                arr = [[text for (text, ident) in features[index:index+i]]];
                                if canReplaceIdent:
                                        indexes = [ind for ind in range(index, index+i) 
                                                   if features[ind][1] == 'i'];
                                        for ind in indexes:
                                                a = arr[0][:];
                                                a[ind - index] = '$ident';
                                                arr += [a];
                                for tmp in arr:
                                        str = ','.join( tmp );
                                        res += ['%s(%s)' % (name, tmp)];
                return res;

        def _extractFromHeader(self, heading):
                text = self._splitText(heading);
                for word in text:
                        self._features.update( self._extractFromIdentifier(word) );
                text = self._normalizeWords(text);
                text = map( lambda x: (x, self._getWordLabel(x)), text );
                for i in range(0, len(text)):
                        self._features.update(self._genNGrams('header', text, i, 5, False));

        def _extractFromIdentifier(self, word):
                res = [];
                parts = re.findall(ParsingRegexps.IdentsParsingRe, word);
                parts = filter( lambda x: len(x) > 0, 
                                reduce( lambda list1, list2: list1 + list2,
                                        [list(x) for x in parts], []) );
                parts = self._normalizeWords( parts );
                if not parts is None and len(parts) > 1:
                        for part in parts:
                                res = res + ['ident(%s)' % part];
                return res;
                
        def _getWordLabel(self, word):
                if self._isIdentifier(word):
                        return 'i';
                elif self._isNumber(word) or word == 'numb3r':
                        return 'n';
                else:
                        return 'p';

        def _extractFromContent(self, content):
                text = self._splitText(content);
                text = self._normalizeWords(text);
                text = map( lambda x: (x, self._getWordLabel(x)), text );
                for i in range(0, len(text)):
                        self._features.update(self._genNGrams('content', text, i, 5, False));
                                
        def _preprocessCode(self, code):
                res = [];
                for token in code:
			res += [token];
                return res;

        def _extractFromCode(self, code):
                text = self._splitText(code);
                for word in text:
                        self._features.update(self._extractFromIdentifier(word));
                text = self._normalizeWords(text);
                text = self._preprocessCode(text);
                text = map( lambda x: (x, self._getWordLabel(x)), text );
                for i in range(0, len(text)):
                        ngrams = self._genNGrams('code', text, i, 5, True);
                        self._features.update(ngrams);
