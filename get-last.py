#!/usr/bin/env python

import pylast
import json
import datetime
from elasticsearch import Elasticsearch

API_KEY = "" 
API_SECRET = ""
user = ""
limit = 200
index = "lastfm"
doc_type = "lastfm"

scrobble = {}
elastichost='localhost:9200'

es = Elasticsearch(elastichost)

network = pylast.LastFMNetwork(api_key=API_KEY, api_secret=API_SECRET)

recent_tracks = network.get_user(user).get_recent_tracks(limit=limit)

for track in recent_tracks:
  trackname = track.track.get_name()
  artist = track.track.get_artist()
  string_ts = str(track.timestamp)
  id = string_ts
  start_date = datetime.datetime.fromtimestamp(int(string_ts)).strftime('%Y-%m-%dT%H:%M:%S')
  scrobble['artist'] = unicode(artist)
  scrobble['track'] = unicode(trackname)
  scrobble['album'] = unicode(track.album)
  scrobble['timestamp'] = start_date
  es.index(index=index, doc_type=doc_type, id=id, body=scrobble)
