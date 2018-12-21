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
import re

def get_params():
 r=requests.get("https://www.betfair.com/exchange/plus/")
 ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()
 return [ak]

[ak]=get_params()

try:
 junk=requests.put("http://localhost:9200/uk_hr_all_build7/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
 junk=1

def betfair_list(event_type_id=7):
 global ak
 r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[event_type_id],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
 r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId="+str(event_type_id)+"&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
 race_list={}
 for t in r.json()['races']:
  if int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple())) in race_list and "(" not in t['meetingName']:
   del race_list[int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple()))] 
  else:
   if "(" not in t['meetingName'] and int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple())) not in race_list:
    race_list[int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple()))]=t['raceId']
 return race_list

def betfair_dat(raceId):
 r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"exchangeIds":[1],"eventTypeIds":[7],"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"attachments":["MARKET_LITE"],"selectBy":"RANK","raceIds":[raceId]},"textQuery":None,"facets":[],"currencyCode":"USD","locale":"en"})
 dat={}
 rs=[grequests.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak="+str(ak)+"&currencyCode=USD&locale=en&marketIds="+str(c)+"&rollupLimit=4&rollupModel=STAKE&types=MARKET_STATE,MARKET_DESCRIPTION,RUNNER_METADATA,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP") for c in [r.json()['attachments']['liteMarkets'][c]['marketId'] for c in r.json()['attachments']['liteMarkets']]]
 tmp1=grequests.map(rs)
 horse_map={}
 for r in tmp1:
  if r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketType']=="WIN":
   horse_map={c['selectionId']:int(c['description']['metadata']['CLOTH_NUMBER']) for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if 'CLOTH_NUMBER' in c['description']['metadata']}
 for r in tmp1:
  dat[r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketType']]={"marketId":r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['marketId'],"marketData":sorted([{"horse_number":int(horse_map[c['selectionId']]),"horse_name":c['description']['runnerName'],"midPrice":round((sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price']+sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price'])/2.0,2),"backPrice":sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price'],"layPrice":sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price']} for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['selectionId'] in horse_map and 'availableToLay' in c['exchange'] and 'availableToBack' in c['exchange']],key=lambda x:x['midPrice'])}
 out={}
 if "WIN" in dat:
  out['betfair_win']=dat['WIN']['marketData']
  out['betfair_win_marketId']=dat['WIN']['marketId']
 else:
  out['betfair_win']=None
  out['betfair_win_marketId']=None
 if "PLACE" in dat:
  out['betfair_place']=dat['PLACE']['marketData']
  out['betfair_place_marketId']=dat['PLACE']['marketId']
 else:
  out['betfair_place']=None
  out['betfair_place_marketId']=None
 if "MATCH_BET" in dat:
  out['betfair_avb']=dat['MATCH_BET']['marketData']
  out['betfair_avb_marketId']=dat['MATCH_BET']['marketId']
 else:
  out['betfair_avb']=None
  out['betfair_avb_marketId']=None
 return out

def racinguk_list():
 r=requests.get("https://www.racinguk.com/racecards")
 soup=BeautifulSoup(r.text,"html.parser")
 urls={}
 for t in soup.find_all(class_="button button--link button--primary"):
  race_time=int(time.mktime(dateutil.parser.parse(datetime.date.today().strftime("%Y-%m-%d")+"T"+t.text.strip()+".000Z").timetuple()))-3600
  if race_time not in urls:
   urls[race_time]="https://www.racinguk.com"+t.get("href")
 return urls

def racinguk_dat(cur_url):
 r=requests.get(cur_url+"/timeform")
 soup=BeautifulSoup(r.text,"html.parser")
 tf_ratings={int(soup.find(class_="card--timeform").find_all(class_="racecard__runner__barrier")[i].text.strip()):i+1 for i in range(len(soup.find(class_="card--timeform").find_all(class_="racecard__runner__barrier")))}
 tf_stats=[]
 for t1 in soup.find_all(class_="card--timeform"):
  if "SMART STATS" in t1.text:
   tf_stats=[int(c.text.strip()) for c in t1.find_all(class_="card__runner__name--number")]
 r=requests.get(cur_url)
 soup=BeautifulSoup(r.text,"html.parser")
 runner_urls=["https://www.racinguk.com"+c.find("a").get("href") for c in soup.find_all(class_="racecard__runner__name") if c.find("a")]
 rs=[grequests.get(c) for c in runner_urls]
 tmp=grequests.map(rs)
 out=[]
 for r in tmp:
  soup1=BeautifulSoup(r.text,"html.parser")
  exp=1
  try:
   junk=[c.find(class_="racecard__form__date").text for c in soup1.find(class_="racecard__form__table").find("tbody").find_all("tr")]
   if len(junk)!=5:
    exp=0
  except:
   exp=0
  if exp:
   out.append(int(soup1.find(class_="racecard__runner__cloth-number").text.strip()))
 fin_out=[]
 for t in soup.find_all(class_="racecard__runner--content"):
  try:
   hr_num=int(t.find(class_="racecard__runner__cloth-number").text.strip())
   hr_name=t.find(class_="racecard__runner__name").find("a").text.strip()
   pacemap=[i for i in range(1,6) if t.find_all(class_="pacemap__position")[i-1].find(class_="pacemap__position__predicted")][0]
   tf_rating=re.sub(r'[^\x00-\x7f]',r'',t.find(class_="timeform__rating").text.strip())
   tf_rating_int=int("".join([c for c in tf_rating if c.isdigit()])) if "".join([c for c in tf_rating if c.isdigit()]) else 0
   odds=round(float(t.find("live-odds").get("data-js-odds-decimal")),2)
   fin_out.append({"tf_rating_int":tf_rating_int,"tf_stats":1 if hr_num in tf_stats else 0,"tf_rating_123":tf_ratings[hr_num] if hr_num in tf_ratings else 0,"odds":odds,"horse_number":hr_num,"horse_name":hr_name,"pacemap":pacemap,"tf_rating":tf_rating,"experience":1 if hr_num in out else 0})
  except:
   junk=1
 return sorted(fin_out,key=lambda x:(-1*x['tf_rating_123'],x['tf_rating_int']))

def update_res():
 r=requests.get("http://localhost:9200/uk_hr_all_build7/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
 for t in r.json()['hits']['hits']:
  tmp=t['_source']
  if "race_details_dat" in tmp:
   if "betfair_avb" in tmp['race_details_dat'] and tmp['race_details_dat']['betfair_avb'] and "result_avb" not in tmp:
    params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['race_details_dat']['betfair_avb'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
    headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
    r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
    winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
    if winners:
     tmp['result_avb']=winners[0]
   if "betfair_win" in tmp['race_details_dat'] and tmp['race_details_dat']['betfair_win'] and "result_win" not in tmp:
    params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['race_details_dat']['betfair_win'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
    headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
    r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
    winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
    if winners:
     tmp['result_win']=winners[0]
   if "betfair_place" in tmp['race_details_dat'] and tmp['race_details_dat']['betfair_place'] and "result_place" not in tmp:
    params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['race_details_dat']['betfair_place'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
    headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
    r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
    winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
    if winners:
     tmp['result_place']=json.dumps(winners)
   junk=requests.post("http://localhost:9200/uk_hr_all_build7/_doc/"+str(t['_id']),json=tmp)

list1=racinguk_list()
list2=betfair_list()
common_list=[c for c in list1 if c in list2]
print "Next Race in ",([c for c in sorted(common_list) if c>int(time.mktime((datetime.datetime.utcnow().timetuple())))][0]-int(time.mktime((datetime.datetime.utcnow().timetuple()))))/60,"mins"

total_dat={}
while True:
  time.sleep(2)
  try:
    update_res()
  except:
    junk=1
  for t in sorted(common_list):
   cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
   if cur_ts>=t-180 and cur_ts<=t+120 and t not in total_dat:
    try:
      final_dat={}
      final_dat['timestamp']=int(t)*1000
      final_dat['1_racinguk']=racinguk_dat(list1[t])
      bf_dat=betfair_dat(list2[t])
      final_dat['betfair_win_dat']=bf_dat['betfair_win']
      final_dat['betfair_place_dat']=bf_dat['betfair_place']
      final_dat['betfair_avb_dat']=bf_dat['betfair_avb']
      final_dat['race_details_dat']={"betfair_place":bf_dat['betfair_place_marketId'],"betfair_avb":bf_dat['betfair_avb_marketId'],"racinguk":list1[t],"betfair_win":bf_dat['betfair_win_marketId']}
      r=requests.post("http://localhost:9200/uk_hr_all_build7/_doc/"+str(final_dat['timestamp']),json=final_dat)
      print r.text
      print list1[t],"Added New Race................."
      total_dat[t]="added"
    except:
      print "Error....."
      junk=1







