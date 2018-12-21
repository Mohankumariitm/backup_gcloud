import requests
import traceback
import time
from pprint import pprint
from bs4 import BeautifulSoup
import dateutil.parser
from datetime import timedelta
import datetime
import threading

model_name="uk_hr_all_build12"

matchbook_username="mohanbet365"
matchbook_password="mohankumar95"

live_session=requests.Session()
matchbook_session=requests.Session()

lay_stake=0.15
lay_odds=50.0
furlong_thresh=7

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

def matchbook_map(cur_race_time):
  r=requests.get("https://api.matchbook.com/edge/rest/lookups/sports")
  horse_race_id=[c for c in r.json()['sports'] if c['name']=="Horse Racing"][0]['id']
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

def commentry_list():
  global live_session
  race_list={}
  r1=live_session.get("http://pasms.pa-sport.com/meetings.aspx")
  soup1=BeautifulSoup(r1.text,"html.parser")
  for t1 in soup1.find(id="ddMeetings").find_all("option"):
    if int(t1.get("value")):
      r2 = live_session.post('http://pasms.pa-sport.com/meetings.aspx',data={"__EVENTVALIDATION":soup1.find(id="__EVENTVALIDATION").get("value"),'ddMeetings':t1.get("value"),"__VIEWSTATE":soup1.find(id="__VIEWSTATE").get("value"),"__VIEWSTATEGENERATOR":soup1.find(id="__VIEWSTATEGENERATOR").get("value")})
      soup2=BeautifulSoup(r2.text,"html.parser")
      for t2 in soup2.find(id="pnlRaces").find_all("option"):
        if int(t2.get("value")):
          race_time=int(time.mktime(dateutil.parser.parse(datetime.date.today().strftime("%Y-%m-%d")+"T"+t2.text.strip()+".000Z").timetuple()))-3600
          race_list[race_time]=int(t2.get("value"))
  return race_list

def commentary_check(race_id):
  global furlong_thresh
  r=requests.get("http://F11BE7880B13463DB3C36772A6FDF8AF:1D2351E160B41BE95E0C3B957FBA2FD5A4F3C09BF020CC5D12CBD07A9ADCB766@paracing.cloudapp.net/api/v1/race/"+str(race_id)+"/commentary?lang=en-GB")
  horses=[]
  if r.text:
    for t in r.json()[::-1]:
      if not t['hasEnded']:
        if t["position"]:
          horses.append(t['runner']['clothNumber'])
        if t["furlong"]<=100 and t['furlong']>furlong_thresh and len(horses)>1:
          return horses
        if t["jump"] and t['jump']<100 and len(horses)>1:
          return horses
      else:
        return -1
  return 0

def slave_worker(tmp):
  global race_list,lay_stake,lay_odds
  print "starting loop...."
  start_time=int(time.time())
  flag=1
  while (int(time.time())-start_time)<900 and flag:
    try:
      event_id=race_list[tmp['timestamp']/1000]
      r=requests.get("http://paracing.cloudapp.net/widget/commentary?clientkey=22B4FA377E9A4159B2B8410B5BD6F700&raceId="+str(event_id)+"&theme=pp&pp=yes&lang=en-GB")
      soup=BeautifulSoup(r.text,"html.parser")
      race_id=soup.find(id="PaRaceId").get("value")
      odds_map={c['horse_number']:c['midPrice'] for c in tmp['betfair_win_dat']}
      matchbook_login()
      matchbook_horse_map=matchbook_map(tmp['timestamp']/1000)
      while (int(time.time())-start_time)<900 and flag:
        try:
          time.sleep(0.2)
          cur=commentary_check(race_id)
          if cur<0:
            print "exiting loop...."
            flag=0
            break
          elif cur==0:
            junk=1
          else:
            pred=sorted([[odds_map[c],c] for c in cur if odds_map[c]<=30.0])[-1][1]
            print "prediction",pred
            flag=0
            offers=[{"runner-id":matchbook_horse_map[pred]['runnerId'],"side":"lay","odds":lay_odds,"stake":lay_stake}]
            r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
            log("live_commentry_bet_placed","success",r.text)
        except:
          log("live_commentry_slave_worker_loop2","Fail",str(traceback.format_exc()))         
    except:
      log("live_commentry_slave_worker_loop1","Fail",str(traceback.format_exc()))

try:
  race_list=commentry_list()
  matchbook_login()
  log("live_commentry_init","Success",str(traceback.format_exc()))
except:
  log("live_commentry_init","Fail",str(traceback.format_exc()))

total_dat=[]
while True:
  time.sleep(2)
  try:
    r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "2model2_pred"}}}}})
    for t in r.json()['hits']['hits']:
      tmp=t['_source']
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      if cur_ts<=tmp['timestamp']/1000 and tmp['timestamp']/1000 not in total_dat and tmp['timestamp']/1000 in race_list:
        total_dat.append(tmp['timestamp']/1000)
        junk=threading.Thread(target=slave_worker, args=(tmp,))
        junk.start()
  except:
    log("live_commentry_main_loop","Fail",str(traceback.format_exc()))






