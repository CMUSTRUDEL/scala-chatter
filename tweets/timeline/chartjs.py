from random import randint
import math

def get_x(axis_t):
    return axis_t * 30

def get_y(item_i):
    return item_i * 15

def get_r(count):
    return max(int(math.log(count ** 3)) - 15, 1)

def make(m, limit, xfn = get_x, yfn = get_y, rfn = get_r):
    ks = sorted(m.keys(), key = lambda k: len(m[k]), reverse = True)
    cs = [(randint(10, 240), randint(10, 240), randint(10, 240)) for _ in range(len(ks))]

    def tabulate_instances(L):
        counts = {}
        for _1, _2, _3, axis_t in L:
            count = 0 if axis_t not in counts else counts[axis_t]
            counts[axis_t] = count + 1
        return counts

    def make_data(i):
        k, c = ks[i], cs[i]
        counts = tabulate_instances(m[k])
        points = [('        { x: %s, y: %s, r: %s }' % (xfn(axis_t), yfn(i), rfn(counts[axis_t]))) for axis_t in counts]
        return \
                '    {\n' +\
                '      data: [\n' +\
                (',\n'.join(points)) +\
                '\n      ],\n' +\
                '      backgroundColor: "rgba(%s, %s, %s, 0.8)",\n' % c +\
                '      borderColor: "rgba(%s, %s, %s, 1)",\n' % c +\
                '      label: "%s"\n' % k +\
                '    }'

    return \
            'const data = {\n' +\
            '  datasets: [\n' +\
            ',\n'.join([make_data(i) for i in range(limit)]) +\
            '\n  ]\n};'

if __name__ == '__main__':
    import sys
    import db

    if len(sys.argv) >= 2:
        if sys.argv[1] == 'hashtag':
            import hashtag_frequency
            import timeline

            axis = db.execute(db.mk_connection(), 'select min(created_at), max(created_at) from tweets')[0]
            limit = int(sys.argv[2]) if len(sys.argv) >= 3 else 15
            m, center_date_map = timeline.make(hashtag_frequency.map(), axis, top = limit)
            def get_x_date(axis_t):
                d = center_date_map[axis_t]
                return '"%s"' % d.strftime('%d/%m/%Y')
            def get_y_reverse(item_i):
                return 10 + (limit - item_i) * 16

            s = make(m, limit, xfn = get_x_date, yfn = get_y_reverse)
            fname = sys.argv[3] if len(sys.argv) >= 4 else 'data.js'
            with open(fname, 'w') as f:
                f.write(s)

        if sys.argv[1] == 'url':
            import url_frequency
            import timeline

            axis = db.execute(db.mk_connection(), 'select min(created_at), max(created_at) from tweets')[0]
            limit = int(sys.argv[2]) if len(sys.argv) >= 3 else 15
            m, center_date_map = timeline.make(url_frequency.map(), axis, top = limit)
            def get_x_date(axis_t):
                d = center_date_map[axis_t]
                return '"%s"' % d.strftime('%d/%m/%Y')
            def get_y_reverse(item_i):
                return 10 + (limit - item_i) * 32

            s = make(m, limit, xfn = get_x_date, yfn = get_y_reverse)
            fname = sys.argv[3] if len(sys.argv) >= 4 else 'data.js'
            with open(fname, 'w') as f:
                f.write(s)

