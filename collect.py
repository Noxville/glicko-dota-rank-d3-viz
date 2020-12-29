from dateutil.parser import parse
from datetime import timedelta
from urllib.request import urlretrieve
import json
import os
import pandas as pd
from zlib import crc32


class Rating:
    def __init__(self, rrating, date):
        self.rating = rrating
        self.date = date
        self.rank = None


class Team:
    def __init__(self, tid, name):
        self.id = tid
        self.name = name
        self.top10s = 0
        self.ratings = dict()

    def add_rating(self, new_rating):
        self.ratings[new_rating.date] = new_rating

    def to_row(self, dates):
        ret = [self.name]
        for _d in dates:
            ret.append(self.ratings[_d].rating if _d in self.ratings else 1.0 - str_to_float(self.name))
        return ret


# make unranked teams deterministically unranked
def str_to_float(s, encoding="utf-8"):
    return bytes_to_float(s.encode(encoding))


def bytes_to_float(b):
    return float(crc32(b) & 0xffffffff) / 2 ** 32


def ensure_data_get_dates():
    dates = []  # set of seen dates
    cur, start = parse('29th December 2020'), parse('6th January 2020')
    fmt = "%d-%m-%Y"

    while cur > start:
        dt = cur.strftime(fmt)
        dates.append(dt)
        path = 'data/{}.json'.format(dt)
        url = 'http://datdota.com/api/ratings?date={}'.format(dt)

        if not os.path.isfile(path):
            urlretrieve(url, path)
        cur -= timedelta(days=7)

    return dates[::-1]  # put in chronological order


def load_data(dates):
    allowed_team_ids = {_: True for _ in
                        [6711290, 6685591, 2586976, 2163, 1838315, 111474, 7220047, 7101094, 7653600, 7555613,
                         7553952, 8077174, 7556672, 8204562, 7554697, 6214973, 8172168]}
    ignored_teams = {_: True for _ in
                     [3586078, 350190, 7079109, 6953913, 6209166, 7203342, 5014799, 2640025, 46, 5055770, 7408440,
                      7186466, 7409177, 7422789, 7059982, 5892936, 7220281, 6187657, 7099096, 7483893, 15,
                      2626685, 726228, 5, 67, 2108395, 7441136, 39, 7441226, 6209804, 3785359, 6209143,
                      1375614, 2672298, 7119077, 6738815, 7119388, 36, 1520578, 4, 7118032, 7098928, 7404529, 7298091,
                      7121518, 7669103, 7640557, 6382242, 7681441, 7407260, 7732977, 7684041, 7748744, 7820540, 7806386,
                      6409189, 7819701, 7390454, 7424172, 7819028, 7913999, 5229568, 6849739, 7267298, 7408845, 7470549,
                      7389602, 4817649, 7453744, 7542148, 7453020, 8021531, 8077147, 7667536, 8021940, 8086218, 7960275,
                      8160726, 8126892, 8145157, 8161113, 8118197, 8143502, 7486089, 8048279, 8204512, 8195614, 8118983,
                      1883502, 8121295]}

    team_map = {}  # id to team
    for dt in dates:
        with open("data/{}.json".format(dt), encoding='utf8') as fin:
            _js = json.load(fin)

            for d in _js['data']:
                team_id = d['valveId']
                team_name = d['teamName']

                if team_id in allowed_team_ids:
                    team = team_map[team_id] if team_id in team_map else Team(team_id, team_name)
                    rating = d['glicko2']['rating']

                    team.add_rating(Rating(rating, dt))
                    team_map[team_id] = team
                else:
                    if team_id not in ignored_teams:
                        print("Ignoring team {} with team_id {}".format(team_name, team_id))
                        ignored_teams[team_id] = True
    return team_map.values()


seen_dates = ensure_data_get_dates()
team_data = load_data(seen_dates)

weeks = list(range(len(seen_dates)))
week_ratings, week_idxs = [str(_) + "_rating" for _ in weeks], [str(_) for _ in weeks]

df = pd.DataFrame([_.to_row(seen_dates) for _ in team_data], columns=['teamname'] + week_ratings)
for week in weeks:
    df[str(week)] = df[str(week) + "_rating"].rank(method='max', ascending=False).astype(int)
df = df.drop(columns=week_ratings)

df['avg_rank'] = df.mean(axis=1, skipna=True)
df = df.sort_values(by=['avg_rank'])
df = df.drop(columns=['avg_rank'])

print(df.to_string())

with open("data.csv", 'w', encoding='utf8', newline='\n') as fout:
    df.to_csv(fout, index=False)
