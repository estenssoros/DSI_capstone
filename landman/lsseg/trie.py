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
General and specialised trie objects.
'''


from __future__ import division

from math import log


class PaddingTag(object):
    '''
    A simple object marking major boundaries (sentences/paragraphs).
    '''
    def __init__(self, desc='PAD'):
        self.desc = desc

    def __repr__(self):
        return self.desc

PAD_SYMBOL = PaddingTag()


class Trie(object):
    '''
    A frequency trie of n-grams.

    Works well with both character and word n-grams.
    '''
    def __init__(self):
        self.root = TrieNode('')
        self._has_cache = False
        self._cached_size = None
        self._cached_len = None

    def add(self, ngram):
        '''
        Store the elements of ngram in the trie.
        Leaf nodes --where the last element of ngram might
        point to-- are empty TrieNode instances.
        '''
        node = self.root
        for elem in ngram:
            node = node.add(elem)
        if self._has_cache and node is not self.root:
            self._has_changed()

    def lookup(self, ngram):
        '''
        Look up an ngram in this trie and return the node
        which follows ngram's last element.

        Returns None if ngram is not present in the trie.
        '''
        node = self.root
        try:
            for elem in ngram:
                node = node[elem]
        except KeyError:
            return None
        else:
            return node

    def freq(self, ngram, next_elem=None):
        '''
        Look up ngram in the trie and return a numeric
        value related to the ngram's frequency.

        If lookup() is None (default) the number of
        observed occurrences is returned.
        If next_elem is specified, the ratio of
            occ(ngram + next_elem) / occ(ngram)
        is returned instead.
        If ngram (or ngram + next_elem respectively) is not
        found in the trie, 0 is returned.
        '''
        node = self.root
        local_count = self.size()
        for elem in ngram:
            try:
                node = node[elem]
                local_count = node.freq
            except KeyError:
                return 0

        if next_elem is None:
            return local_count
        else:
            try:
                return node[next_elem].freq / local_count
            except KeyError:
                return 0

    def size(self):
        '''
        The trie's size is defined as the number of n-grams
        that have been stored so far.
        Duplicates are not removed.  To count the number of
        distinct paths in a trie, use::
            len(list(some_trie.iterpath()))
        To get the number of nodes in a trie, use::
            len(some_trie)
        '''
        if self._cached_size is None:
            self._cached_size = sum(c.freq for c in self.root.children())
            self._has_cache = True
        return self._cached_size

    def freedoms(self, ngram):
        '''
        Return a list of freedom values for the given ngram.
        If ngram is not represented completely in the trie,
        a KeyError is raised.
        The returned list's length is len(ngram) + 1, since
        the root node has a freedom value too.
        '''
        node = self.root
        fs = [node.freedom()]
        for elem in ngram:
            node = node[elem]
            fs.append(node.freedom())
        return fs

    def iterpath(self):
        '''
        Iterate through all paths in the trie, in arbitrary
        order.
        Each path is yielded as a list of TrieNodes.

        See also Trie.walk().
        '''
        return self.root.iterpath()

    def _has_changed(self):
        '''
        After any change, reset all cached values.
        '''
        self._cached_size = None
        self._cached_len = None
        self._has_cache = False

    def walk(self):
        '''
        Iterate over all nodes in this trie in a top-down,
        depth-first fashion.

        See also Trie.iterpath().
        '''
        return self.root.walk()

    def __len__(self):
        '''
        Return the number of nodes in this tree.
        This operation is expensive on first call after any
        change, since the complete tree is traversed.

        To get the number of stored n-grams, use::
            some_trie.size()
        To get the number of distinct paths in a trie
        (ie. n-gram types), use::
            len(list(some_trie.iterpath()))
        '''
        if self._cached_len is None:
            self._cached_len = sum(1 for _ in self.root.walk())
            self._has_cache = True
        return self._cached_len

    def __repr__(self, name='Trie'):
        '''
        A compact representation of the complete trie in
        the format '<Trie {elem:freq:{children}}>'.
        The output string cannot be turned back into a
        Trie object.

        However, if the trie is rather large its size only
        is indicated rather than its content.
        For performance reason, Trie.size() is used for
        extimating largeness, rather than Trie.__len__().
        '''
        if self.size() <= 10:
            return u'<%s %s>' % (name, repr(self.root))
        else:
            return u'<%s of size %d>' % (name, self.size())


class TrieNode(dict):
    '''
    A node of a Trie.
    TrieNode inherits from dictionary.

    The dictionary entries of TrieNode are its children.
    Its keys are the labels of the outgoing transitions,
    the values are child nodes.
    Every node also has a backlink to its (only) parent.
    A root node is expected to have its parent property
    set to None.

    The node holds all properties of the ingoing edge. Thus
    eg. node.elem is the label of the ingoing transition.
    However, the freedom() method returns the number of
    outgoing transitions, since it is not a property of a
    single edge.  To get the freedom value preceding this
    node, call the freedom() method of the node's parent.
    '''
    def __init__(self, elem, parent=None):
        super(TrieNode, self).__init__()
        self.elem = elem
        self.parent = parent
        self.freq = 0  # frequency of transitions ending here

    def add(self, elem):
        '''
        Add an element to an edge starting from this node.
        If there is no outgoing transition with the given
        element, a new edge is created.
        Return the child node that is pointed to by elem.
        '''
        child = self.setdefault(elem, TrieNode(elem, self))
        child.freq += 1
        return child

    def entropy(self):
        '''
        Return the entropy of the outgoing edges.
        '''
        freqs = [c.freq for c in self.children()]
        N = sum(freqs)
        probs = [f / N for f in freqs]
        return -sum(p * log(p, 2) for p in probs)

    def children(self):
        '''
        Return a list of all immediate descendants.
        '''
        return self.values()

    def freedom(self):
        '''
        A node's freedom is defined as the number of
        outgoing nodes (Wrenn et al. 2007).  Thus, this
        is equivalent to the node's length.
        '''
        return len(self)

    def deeplength(self):
        '''
        Return the number of nodes in the subtree under
        this node.
        This operation is expensive, since the complete
        subtree is traversed.
        '''
        return sum(1 for _ in self.walk())

    def iterpath(self):
        '''
        Iterate over all paths starting from this node.
        If this is a leaf node, return a single-item list
        containing this node only.
        '''
        if self:
            for child in self.children():
                for path in child.iterpath():
                    yield [self] + path
        else:
            yield [self]

    def walk(self):
        '''
        Iterate over the subtree under this node, ie. this
        node and all of its (transitive) descendants, in a
        top-down, depth-first fashion.
        '''
        yield self
        for subtree in self.children():
            for child in subtree.walk():
                yield child

    def __repr__(self, name='TrieNode'):
        '''
        A compact representation of the node and its
        children in the format 'elem{freq:child, ...}'.
        This method is recursive, so the complete subtree
        under the node is shown.

        However, if the subtree under this node is rather
        large an indication of its size is given only.
        Largeness is estimated by the number of children.
        '''
        if len(self) < 10:
            return '%s{%s}' % (repr(self.elem), ', '.join(
                ':'.join(repr(x) for x in [c.freq, c])
                for c in self.children()))
        else:
            return '<%s %s with %d children>' % (
                name, repr(self.elem), len(self))

    def __str__(self):
        return str(self.elem)


class CharTrie(Trie):
    '''
    Subclass of Trie, adds methods for dumping the Trie's
    data, for quicker re-building.
    '''
    def __init__(self):
        super(CharTrie, self).__init__()
        self.root = CharTrieNode('')

    def dumpdata(self):
        '''
        Yield each of the CharTrie's n-grams as a string.

        The string consists of two Python expressions,
        representing the n-gram and its frequency, separated
        by a tab and terminated by a newline.

        The first expression evaluates to str or unicode if
        the n-gram's elements are characters only, otherwise
        (ie. if there are PaddingTag objects present) it is
        written as a list, using the built-in repr().

        NB: The PaddingTags' representation must be chosen
        to be valid Python identifiers, so that they can be
        interpreted by eval() using the globals key-arg.
        '''
        # Normal case: yield all branches (ie. paths to leaves).
        for path in self.iterpath():
            yield self._dumpformat(path, path[-1].freq)

        # Find n-grams that end at non-leaves.
        # There shouldn't be any if the n-grams were entered with
        # PaddingTags properly.
        for node in self.walk():
            if node and node.freq:  # no leaves and no root
                outgoingfreqs = sum(c.freq for c in node.children())
                if node.freq != outgoingfreqs:
                    path = [node]
                    n = node.parent
                    while n is not None:  # root's parent is None
                        path.insert(0, n)
                        n = n.parent
                    yield self._dumpformat(path, node.freq - outgoingfreqs)

    @staticmethod
    def _dumpformat(path, freq):
        try:
            # Save some space: make a str/unicode if possible.
            chars = repr(''.join(n.elem for n in path))  # ignore root's ''
        except TypeError:
            chars = repr([n.elem for n in path[1:]])  # skip root's ''
        return '%s\t%d\n' % (chars, freq)

    def loaddump(self, dump, glbls={repr(PAD_SYMBOL): PAD_SYMBOL}):
        '''
        Load data from a dump into this CharTrie.

        dump must be an iterable of strings as produced by
        CharTrie.dumpdata(). The glbls dictionary can be
        used to restore PaddingTags.

        Return the maximal trie depth.
        '''
        depth = 0
        for line in dump:
            sequence, freq = (eval(x, glbls) for x in line.split('\t'))
            depth = max(depth, len(sequence))
            self._load(sequence, freq)
        self._has_changed()
        return depth

    def _load(self, ngram, freq):
        node = self.root
        for char in ngram:
            node = node.add(char, freq)

    def remove(self, ngram, freq=1):
        '''
        Undo the adding of an n-gram.
        '''
        node = self.root
        for char in ngram:
            try:
                node = node.remove(char, freq)
            except AttributeError:
                # Complete subtree removed before end of ngram was reached.
                break
        if self._has_cache and node is not self.root:
            self._has_changed()

    def __repr__(self, name='CharTrie'):
        return super(CharTrie, self).__repr__(name)


class CharTrieNode(TrieNode):
    '''
    Subclass of TrieNode, overrides add() method to allow
    for adding frequencies > 1.
    '''
    def __init__(self, char, parent=None):
        super(CharTrieNode, self).__init__(char, parent)

    def add(self, char, freq=1):
        '''
        CharTrieNode.add(char, freq) has the same effect as
        calling TrieNode.add(char) freq times.
        '''
        child = self.setdefault(char, CharTrieNode(char, self))
        child.freq += freq
        return child

    def remove(self, char, freq=1):
        '''
        Undo the adding of a character.
        '''
        child = self[char]
        child.freq -= freq
        if child.freq <= 0:
            del self[char]  # remove subnode
            del child       # free memory
            return None
        return child

    def __repr__(self, name='CharTrieNode'):
        return super(CharTrieNode, self).__repr__(name)
