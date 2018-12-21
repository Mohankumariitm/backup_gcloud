import json
import requests
import datetime
import time
import pickle
import base64
from bs4 import BeautifulSoup
import dateutil.parser
from datetime import timedelta
import sys, os
import calendar
import grequests
from pprint import pprint
import traceback

lay_odds_1=50.0
lay_stake_1=1.01
lay_odds_2=50.0
lay_stake_2=0.12
lay_odds_3=100.0
lay_stake_3=0.13

model_name="skyracing_uk"
matchbook_username="mohanbet365"
matchbook_password="mohankumar95"
matchbook_session=requests.Session()

try:
  junk=requests.put("http://localhost:9200/logs/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

def log(error_type,error_result,error_msg):
  print error_type,error_result,error_msg
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":error_type,"error_msg":error_msg})
  except:
    junk=1

def matchbook_login():
  for i in range(5):
    try:
      global matchbook_session,matchbook_username,matchbook_password
      r=matchbook_session.get("https://api.matchbook.com/bpapi/rest/security/session")
      if r.status_code!=200:
        r=matchbook_session.post("https://api.matchbook.com/bpapi/rest/security/session",json={"username":matchbook_username,"password":matchbook_password})
      return 0
    except:
      junk=1
  log("matchbook_login","Fail",str(traceback.format_exc()))

def matchbook_params():
  r=requests.get("https://api.matchbook.com/edge/rest/lookups/sports")
  return [c for c in r.json()['sports'] if c['name']=="Horse Racing"][0]['id']

def matchbook_map(cur_race_time):
  global horse_race_id
  for i in range(5):
    try:
      r=requests.get("https://api.matchbook.com/edge/rest/events?category-ids="+str(horse_race_id))
      out={}
      for t in r.json()['events']:
        if {c['type']:c['name'] for c in t['meta-tags']}['COUNTRY'].lower().strip() in ["uk","ireland"]:
          out[int(time.mktime(dateutil.parser.parse(t['start']).timetuple()))]={"race_name":t['name'],"id":t['id']}
      eventId=out[cur_race_time]['id']
      r=requests.get("https://www.matchbook.com/edge/rest/events/"+str(eventId)+"?language=en&currency=USD&exchange-type=back-lay&odds-type=DECIMAL&include-markets=true&include-prices=true&include-runners=true&market-per-page=500&price-depth=6&include-event-participants=true")
      out={}
      for market in r.json()['markets']:
        if market['name']=="WIN":
          for runner in market['runners']:
            horse_num=int(runner['name'].split(" ")[0].strip())
            out[horse_num]={"eventId":eventId,"runnerId":runner['id']}
      return out
    except:
      junk=1
  return 0

def model_1(tmp):
  global lay_odds_1,lay_stake_1
  horse_ratings={c['horse_number']:0 for c in tmp['horse_data']}
  horse_map={c['horse_number']:[c['horse_name'],c['win_lay'],c['place_lay']] for c in tmp['horse_data']}
  for t1 in tmp['ratings_auto']:
    for t2 in t1:
      if t2 in horse_ratings:
        horse_ratings[t2]=horse_ratings[t2]+t1[::-1].index(t2)+1
  ratings=sorted([[horse_ratings[c],c,horse_map[c]] for c in horse_ratings if c in horse_map],reverse=True)
  if ratings[0][2][1]>=10.0 and ratings[0][2][1]<=40.0:
    pred=ratings[0][1]
    matchbook_horse_map=matchbook_map(int(tmp['timestamp']/1000))
    offers=[{"runner-id":matchbook_horse_map[pred]['runnerId'],"side":"lay","odds":lay_odds_1,"stake":lay_stake_1}]
    r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
    log("bet_placed","success",r.text)

def model_2(tmp):
  global lay_odds_2,lay_stake_2
  horse_ratings={c['horse_number']:0 for c in tmp['horse_data']}
  horse_map={c['horse_number']:[c['horse_name'],c['win_lay'],c['place_lay']] for c in tmp['horse_data']}
  ratings=[c for c in sorted(tmp['horse_data'],key=lambda x:(x['rating_1'],x['rating_2'])) if c['rating_1'] and c['rating_2']]
  if ratings[0]['win_lay']>=20.0 and ratings[0]['win_lay']<=50.0:
    pred=ratings[0]['horse_number']
    matchbook_horse_map=matchbook_map(int(tmp['timestamp']/1000))
    offers=[{"runner-id":matchbook_horse_map[pred]['runnerId'],"side":"lay","odds":lay_odds_2,"stake":lay_stake_2}]
    r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
    log("bet_placed","success",r.text)

def model_3(tmp):
  global lay_odds_3,lay_stake_3
  horse_ratings={c['horse_number']:0 for c in tmp['horse_data']}
  horse_map={c['horse_number']:[c['horse_name'],c['win_lay'],c['place_lay']] for c in tmp['horse_data']}
  ratings=sorted([c for c in tmp['horse_data'] if c['rating_1'] and c['rating_2']],key=lambda x:abs(x['rating_2']+x['rating_1']))
  if ratings[0]['win_lay']>=20.0 and ratings[0]['win_lay']<=100.0 and ratings[0]['rating_1']+ratings[0]['rating_2']<140:    
    pred=ratings[0]['horse_number']
    matchbook_horse_map=matchbook_map(int(tmp['timestamp']/1000))
    offers=[{"runner-id":matchbook_horse_map[pred]['runnerId'],"side":"lay","odds":lay_odds_3,"stake":lay_stake_3}]
    r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
    log("bet_placed","success",r.text)

horse_race_id=matchbook_params()
matchbook_login()
log("matchbook_init","Success",None)

total_dat=[]
while True:
  time.sleep(2)
  try:
    r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "2model2_pred"}}}}})
    for t in r.json()['hits']['hits']:
      tmp=t['_source']
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      if cur_ts<=tmp['timestamp']/1000+120 and cur_ts>=(tmp['timestamp']/1000)-300 and tmp['timestamp']/1000 not in total_dat:
        matchbook_login()
        print "New Race......................."
        model_1(tmp)
        #model_2(tmp)
        #model_3(tmp)
        total_dat.append(tmp['timestamp']/1000)
  except:
    log("matchbook_loop","Fail",str(traceback.format_exc()))









