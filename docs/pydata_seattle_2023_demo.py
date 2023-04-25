import ibis
from ibis import _
from ibis import selectors as s
from ibis.interactive import *  # noqa: F403

# 1. Load some baseball data
pgcon = ibis.connect("postgres://nixcloud:@127.0.0.1:54321/nixcloud")
batting = pgcon.tables.batting
batting

# complex selector
#
# 2. How do I normalize all numeric columns?
normed = batting.mutate(
    s.across(~s.r[:"lgID"] & s.numeric(), (_ - _.mean()) / _.std()),
    G_orig=_.G,
).alias("normed")
normed

# 3. Ok, but what if I want to use a function that isn't available in ibis?
#                    I am forced to use SQL
#                    I like SQL
med = normed.sql(
    """
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "G_orig") AS "G_med"
    FROM normed
    """
)
mad = (
    normed.cross_join(med)
    .mutate(G_mad=(_.G_orig - _.G_med).abs())
    .select(s.r[:5], "G_orig", "G_med", "G_mad", G_norm=_.G)
)
mad

# 4. join with a pandas thing
#
# ok, so what if your boss hates ibis *and* your co workers hate SQL?
import pandas as pd

awards_players = pd.read_csv(
    "/home/cloud/data/pydata/seattle/2023/awards_players.csv"
).replace({float("nan"): None})

# transparent join with dataframes!
mad_awards = mad.join(awards_players, ["playerID", "yearID", "lgID"])
mad_awards

# 5. what if that table is in another DB, and not a local csv??
#
# ok, so turns out your ops team also hates postgres you to suffer and decides
# to put some data in another db but keep postgres around for compatibility
mscon = ibis.connect("mysql://ibis:ibis@localhost:3306/ibis_testing")
awards_players = mscon.tables.awards_players

mad_awards = mad.join(awards_players, ["playerID", "yearID", "lgID"])
# mad_awards
mad_awards

# 6. what if I just want to watch the world burn?
# at this point your just like "fuck it"
# ... um wat?
sql = ibis.to_sql(mad_awards)
expr = ibis.parse_sql(sql)
print(ibis.decompile(expr))


# 7. have you lost your mind?
