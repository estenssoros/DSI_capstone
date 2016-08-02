#!/usr/local/bin/python
# coding: utf8

# lsseg -- Unsupervised Segmentation Using Letter Successor Counts
# Copyright 2013-2014 Lenz Furrer <lenz.furrer@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>


'''
Segmenting utitlities.
'''


from collections import deque

from .trie import CharTrie, PAD_SYMBOL


def text_to_tries(text, n=4, forward=CharTrie(), backward=CharTrie(),
                  lowercase=False, undo=False, pad_symbol=PAD_SYMBOL):
    '''
    Read all of text's character n-grams into a forward and
    a backward trie.

    If lowercase evaluates to True, normalise all input to
    lower case before adding it.

    With the undo flag set to True, remove the n-grams
    rather than adding them.
    '''
    if lowercase:
        normalise = lambda x: x.lower()
    else:
        normalise = lambda x: x
    if undo:
        fw_add, bw_add = forward.remove, backward.remove
    else:
        fw_add, bw_add = forward.add, backward.add

    ngram = deque([pad_symbol], n)
    missing = max(0, n - 2)
    for char in text:
        ngram.append(normalise(char))
        if missing:  # n-grams of size < n at start -> backward only
            missing -= 1
            bw_add(reversed(ngram))
        else:
            fw_add(ngram)
            bw_add(reversed(ngram))
    ngram.append(pad_symbol)
    bw_add(reversed(ngram))
    while len(ngram) > 1:  # n-grams of size < n at end -> forward only
        fw_add(ngram)
        ngram.popleft()
    return forward, backward


class Segmenter(object):
    '''
    Naive approach to segment text, given a forward and a
    backward character trie that were derived from that text.

    window is the depth of the tries, ie. the maximal length
    of character n-grams that can be looked up.

    If lowercase evaluates to True, the input is converted
    to lower case before looking it up in the tries.  The
    segmented output, however, is *not* lowercased.
    '''

    def __init__(self, fwtrie, bwtrie,
                 window=4, peak='freedom', lowercase=False,
                 pad_symbol=PAD_SYMBOL):
        self._fw = fwtrie
        self._bw = bwtrie
        self._pad_symbol = pad_symbol
        self._window = int(window)  # make sure this is int.
        if peak == 'freedom':
            self._peak = self._freedompeak
        elif peak == 'entropy':
            self._peak = self._entropypeak
        else:
            raise ValueError(
                'Key-arg peak must be one of ("freedom", "entropy").')
        if lowercase:
            self._normalise = lambda x: x.lower()
        else:
            self._normalise = lambda x: x

    def segment(self, text, threshold=1):
        '''
        Segment text into pieces.

        Boundaries are inserted where either of the tries shows
        a peak that is greater than or equal to the threshold.
        Where both readings show a peak, their sum is compared
        to the threshold.
        '''
        boundaries = []
        fwtext = list(self._normalise(text))
        bwtext = list(reversed(self._normalise(text)))
        m = len(text)
        # Look at every possible split position i (1..m-1).
        for i in range(1, m):
            # Sum over the peaks in forward and backward reading.
            peaksum = 0
            for txt, trie, pos in ((fwtext, self._fw, i),
                                   (bwtext, self._bw, m - i)):
                peaksum += self._peak(txt, trie, pos - self._window + 1, pos)
                if peaksum >= threshold:
                    boundaries.append(i)
                    break  # shortcut if one peak is enough
        # Split the text at the boundaries found.
        segments = []
        last = 0
        for b in boundaries + [None]:  # There are len(boundaries)+1 segments.
            segments.append(text[last:b])
            last = b
        return segments

    def _entropypeak(self, text, trie, start, stop):
        return trie.lookup(self._pad(text, start, stop)).entropy()

    def _freedompeak(self, text, trie, start, stop):
        # Consider maximum context for each side of the peak.
        # Thus, increase and decrease have different n-gram lookups.
        increase = max(0, self._frdiff(self._pad(text, start, stop), trie))
        if increase:  # Shorcut: No need to calculate decrease without increase.
            decrease = max(0, -self._frdiff(
                self._pad(text, start + 1, stop + 1), trie))
            if decrease:
                return increase + decrease
        return 0

    @staticmethod
    def _frdiff(ngram, trie):
        preleaf = trie.lookup(ngram)
        return preleaf.freedom() - preleaf.parent.freedom()

    def _pad(self, text, start, stop):
        '''
        Produce text[start:stop], with leftpadding if start is negative.
        '''
        if start < 0:
            return [self._pad_symbol] + text[:stop]
        else:
            return text[start:stop]
