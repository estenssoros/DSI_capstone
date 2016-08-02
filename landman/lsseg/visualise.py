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
Visualise text segments by alternate shading.
'''


from itertools import izip_longest


def text_to_HTML(lines, separator=' ', replacement=u'\xA0', **kwargs):
    '''
    Create an HTML visualisation from serialised segmented text.

    `lines` should be an iterable of strings.
    '''
    segments = ((seg.replace(replacement, separator)
                 for seg in line.split(separator))
                for line in lines)
    return seg_to_HTML(segments, **kwargs)


def seg_to_HTML(lines, rowtitles=(), shading='LightGray',
                seg_font='Linux Libertine O', title_font='LMMono10'):
    '''
    Visualise segments with alternating shading in an HTML page.

    `lines` should be an iterable of an iterable of segments.
    '''
    htmldoc = DOC % (title_font, seg_font, shading)
    rows = []
    for segments, rowtitle in izip_longest(lines, rowtitles, fillvalue=''):
        rows.append(ROW % (rowtitle, format_line(segments)))
    return htmldoc % '\n    '.join(rows)


def format_line(segments):
    '''
    Format a line of segments with alternating shading markup.
    '''
    put, save = GRAY, WHITE
    formatted = ''
    for seg in segments:
        formatted += put % seg.encode('ascii', 'xmlcharrefreplace')
        put, save = save, put
    return '<span>%s</span>' % formatted


DOC = '''<!--?xml version="1.0" encoding="utf-8"?-->
<html>
<head>
  <style type="text/css">
    .segments { vertical-align:top; font-family:%s; }
    .rowtitle { font-family:%s; }
    .gray { background-color:%s; }
  </style>
</head>
<body>
  <table>
    %%s
  </table>
</body>
</html>
'''
ROW = '<tr><td class="rowtitle">%s</td><td class="segments">%s</td></tr>'
GRAY = '<span class="gray">%s</span>'
WHITE = '%s'
