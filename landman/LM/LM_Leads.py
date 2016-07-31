def score_word(word, trie):
    if len(word) == 0:
        return 0
    letter = word[0]
    if letter in trie:
        return 1 + score_word(word[1:], trie[letter])
    else:
        return 0


def lsv(word, trie):
    if len(word) == 0:
        return len(trie)

    letter = word[0]
    if letter in trie:
        return lsv(word[1:], trie[letter])
    else:
        return len(trie)


def weight_node(trie):
    weight = 0
    for k, v in trie.iteritems():
        if k == '_end_':
            return 1
        else:
            weight += weight_node(trie[k])
    return weight


def entropy(trie):
    n_weight = weight_node(trie)
    return sum([weight_node(n) / n_weight * np.log(weight_node(n)) / n_weight for c, n in trie.iteritems()])


def in_trie(segment, trie):
    if len(segment) == 0:
        return True
    letter = segment[0]
    if letter in trie:
        return in_trie(segment[1:], trie[letter])
    else:
        return False


def in_windows(windows, trie):
    return [w for w in windows if in_trie(w, trie)]


def text_windows(text, window):
    start = 0
    end = start + window
    tries = []
    while end <= len(text):
        tries.append(text[start:end])
        start += 1
        end += 1
    return tries


def make_trie(text, window=7):
    tries = text_windows(text, window)
    # tries = text.split()
    _end = '_end_'
    root = dict()
    for trie in tries:
        current_dict = root
        for letter in trie:
            current_dict = current_dict.setdefault(letter, {})
        current_dict[_end] = _end
    return root



if __name__ == '__main__':
    docs = [f for f in os.listdir('textdocs/') if f.endswith('.txt')]
    for doc in docs:
        correct = read_text('train_text/{}'.format(doc))
        bad = read_text('textdocs/{}'.format(doc))
        n = len(correct)
        E = distance(correct, bad)
        print '{0} character accuracy: {1:.2f}%'.format(doc, (n - E) / n)

    for doc in docs:
        text = read_text('train_text/{}'.format(doc))
        trie = make_trie(text)
        with open('tries/{0}_f.json'.format(doc.replace('.txt', '')), 'w') as f:
            json.dump(trie, f)
        trie = make_trie(text[::-1])
        with open('tries/{0}_b.json'.format(doc.replace('.txt', '')), 'w') as f:
            json.dump(trie, f)

    with open('tries/DOC100S1082_0_f.json') as f:
        f_trie = json.load(f)

    with open('tries/DOC100S1082_0_b.json') as f:
        b_trie = json.load(f)




# final = []
# i = 0
# found_arr = [w[1] for w in word_arr]
# word_arr = [w[0] for w in word_arr]
# while i <= len(word_arr):
#     if found_arr[i] == False:
#         b = i - 1
#         while b > 0:
#             if len(word_arr[b]) > 3:
#                 b += 1
#                 break
#             else:
#                 b -= 1
#
#         f = i + 1
#         while f < len(word_arr):
#             if len(word_arr[f]) > 3:
#                 break
#             else:
#                 f += 1
#         print word_arr[b - 3:f + 3]
#         print ''.join(word_arr[b:f])
#         distances = [distance(''.join(word_arr[b:f]), word) for word in vocab]
#         print vocab[np.argmin(distances)]
#         time.sleep(3)
#     i += 1
# all_grams = Counter()
# for doc in docs:
#     text = read_text(doc)
#     all_grams.update(gen_ngrams(text))
#
# # correct word_array
# idx = 0
# while idx < len(word_arr) - 3:
#     test_arr = word_arr[idx:idx+3]
#     print test_arr
#     permute_test = list(itertools.product(*test_arr))
#     print permute_test
#     calcs = [calc_thresh(s) for s in permute_test]
#     idx += 3
