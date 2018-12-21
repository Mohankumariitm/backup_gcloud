import time
import calendar
import requests
import grequests
from bs4 import BeautifulSoup
import thread
import datetime
import ast
import itertools
from pprint import pprint
import pickle
import pytz
import dateutil.parser
import traceback
from ngram import NGram

min_stake=0.13
model_name="betfair_sportbook_uk_build3"
r_s=requests.session()
r=r_s.get("https://www.betfair.com/sport/tennis")
bffp="7489"
ak="nzIFcwyWhrlwYMrh"

try:
  junk=requests.put("http://localhost:9200/"+str(model_name)+"/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

def log(error_type,error_result,error_msg):
  print error_type,error_result,error_msg,model_name
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":model_name+"_"+error_type,"error_msg":error_msg})
  except:
    junk=1

def login():
  global r_s
  headerss={"Host":"identitysso.betfair.com","Connection":"keep-alive","Cache-Control":"max-age=0","Origin":"https://www.betfair.com","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36","Content-Type":"application/x-www-form-urlencoded","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8","Referer":"https://www.betfair.com/sport","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.8"}
  params={"product":"sportsbook","redirectMethod":"POST","url":"https://www.betfair.com/sport/login/success?rurl=https://www.betfair.com/sport","username":"mohankumarbet365@gmail.com","password":"mohankumar95"}
  r=r_s.post("https://identitysso.betfair.com/api/login",data=params,headers=headerss)

def update_bffp():
  global r_s,bffp
  r=r_s.get("https://www.betfair.com/sport/tennis")
  soup = BeautifulSoup(r.text, 'html.parser')
  t=soup.find_all(rel="shortcut icon")[0].get('href')
  bffp=t[t.index('favicon_')+8:t.index("_.ico")]

def get_balance():
  global r_s
  headerss={"Host":"www.betfair.com","Connection":"keep-alive","X-Application":"SharedSiteComponent","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36","X-BF-Jurisdiction":"international","Content-Type":"application/json","Accept":"*/*","Accept-Encoding":"gzip, deflate, sdch, br","Accept-Language":"en-US,en;q=0.8"}
  r=r_s.get("https://www.betfair.com/wallet-service/v3.0/wallets?walletNames=[MAIN,SPORTSBOOK_BONUS]&alt=json",headers=headerss)
  return r.json()[0]['details']['amount']

def get_xsrf():
  global r_s
  return r_s.cookies.get_dict()['xsrftoken']

def update_params():
  global ak
  r=requests.get("https://www.betfair.com/exchange/plus/")
  ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()

def get_score(eventId):
  r=r_s.get("https://ips.betfair.com/inplayservice/v1/scores?regionCode=UK&_ak="+str(ak)+"&alt=json&locale=en_GB&eventIds="+str(eventId)+"&ts="+str(calendar.timegm(time.gmtime()))+"&xsrftoken="+get_xsrf())
  cur_score={"current_set":r.json()[0]['currentSet'],"current_game":r.json()[0]['currentGame'],"away":int(r.json()[0]['score']['away']['games']),"home":int(r.json()[0]['score']['home']['games'])}
  return cur_score

def get_data(cur_url):
  global r_s
  try:
    ids={c.split("=")[0]:c.split("=")[1] for c in cur_url.split("?")[1].split("&")}
    r=r_s.get("https://www.betfair.com/sport/horse-racing/meeting?exchangeWinMarketId="+str(ids["exchangeWinMarketId"])+"&eventId="+str(ids['eventId'])+"&marketType=MATCH_BET&raceTime="+str(ids['raceTime'])+"&action=loadRacecardTab&racecardTabType=MATCH_BET&dayToSearch="+str(ids['dayToSearch'])+"&modules=racecard@1090&d18=Main&d31=Middle&isAjax=true&ts="+str(calendar.timegm(time.gmtime()))+"&alt=json&xsrftoken="+get_xsrf())
    html_source=r.json()['page']['config']['instructions'][0]['arguments']['html']
    soup = BeautifulSoup(html_source, 'html.parser')
    cur_runner=soup.find_all(class_="match-bet-market")[0].find_all("a")[0]
    for c1 in r.json()['page']['config']['instructions'][4]['arguments']:
      if c1['marketId']==cur_runner.get('data-marketid'):
        odds_map={str(c['selectionId']):c['prices']['back'][0]['fraction'] for c in c1['runners']}
    markets=[]
    for t in soup.find_all(class_="match-bet-market"):
      try:
        cur_runner=t.find_all("a")[0]
        tmp={c.split("=")[0]:"=".join(c.split("=")[1:]) for c in cur_runner.get("href").split("?")[1].split("&")}
        tmp1={"horse_name":cur_runner.find_previous(class_="runner-name-value").text.strip(),"odds":float(cur_runner.text.replace("\n","").replace(" ","")),"den":odds_map[cur_runner.get('data-selectionid')]['denominator'],"num":odds_map[cur_runner.get('data-selectionid')]['numerator'],"bsmSt":tmp["bsmSt"],"gaZone":tmp['gaZone'],'price':cur_runner.text.replace("\n","").replace(" ",""),'eventId':cur_runner.get("data-eventid"),'bseId':cur_runner.get("data-eventid"),'bssId':cur_runner.get('data-selectionid'),'bsmId':cur_runner.get('data-marketid'),'bsUUID':cur_runner.get('data-uuid'),'bsGroup':cur_runner.get("data-eventid")}
        cur_runner=t.find_all("a")[1]
        tmp2={"horse_name":cur_runner.find_previous(class_="runner-name-value").text.strip(),"odds":float(cur_runner.text.replace("\n","").replace(" ","")),"den":odds_map[cur_runner.get('data-selectionid')]['denominator'],"num":odds_map[cur_runner.get('data-selectionid')]['numerator'],"bsmSt":tmp["bsmSt"],"gaZone":tmp['gaZone'],'price':cur_runner.text.replace("\n","").replace(" ",""),'eventId':cur_runner.get("data-eventid"),'bseId':cur_runner.get("data-eventid"),'bssId':cur_runner.get('data-selectionid'),'bsmId':cur_runner.get('data-marketid'),'bsUUID':cur_runner.get('data-uuid'),'bsGroup':cur_runner.get("data-eventid")}
        markets.append([tmp1,tmp2] )  
      except:
        junk=1
    return markets
  except:
    print str(traceback.format_exc())
    return None

def json_parse(json_input, lookup_key):
  if isinstance(json_input, dict):
    for k, v in json_input.iteritems():
      if k == lookup_key:
        yield v
      else:
        for child_val in json_parse(v, lookup_key):
          yield child_val
  elif isinstance(json_input, list):
    for item in json_input:
      for item_val in json_parse(item, lookup_key):
        yield item_val

def get_list():
  rlist={}
  for i in range(2):
    r=requests.get("https://www.betfair.com/sport/horse-racing?action=loadTab&tab=TODAY&dayToSearch="+str((datetime.datetime.now()+datetime.timedelta(days=i)).strftime("%Y%m%d"))+"&modules=multipick-horse-racing")
    soup = BeautifulSoup(r.text, 'html.parser')
    for t in soup.find_all(class_="meeting-item"):
      if t.get("data-country-name") in ['gb','ie']:
        for t1 in t.find_all(class_="outbound-link"):
          cur_url="https://www.betfair.com"+t1.get("href")
          tmp={c.split("=")[0]:c.split("=")[1] for c in cur_url.split("?")[1].split("&")}
          if "raceTime" in tmp:
            rlist[int(tmp["raceTime"])/1000]=cur_url
  out={}
  prev=0
  for t in sorted(rlist):
    if prev and abs(prev-t)<=18000:
      out[t]=rlist[t]
      prev=t
    elif not prev:
      out[t]=rlist[t]
      prev=t      
    else:
      break
  return out

def update_result():
  global model_name
  r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
  if 'hits' in r.json() and 'hits' in r.json()['hits']:
    for t1 in r.json()['hits']['hits']:
      tmp=t1['_source']
      if ("result" not in tmp or not tmp['result']) and "model_match" in tmp and tmp['model_match']:
        try:
          r1=r_s.post("https://myactivity.betfair.com/activity/sportsbook",json={"status":"SETTLED","dateFilter":90,"fromRecord":0,"pageSize":50,"oddsType":"decimal","firstView":False})
          results={c['betId']:c['status'] for c in r1.json()['bets']}
          if tmp['model_bet_id'] in results:
            tmp['result']=results[tmp['model_bet_id']]
          junk=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(t1['_id']),json=tmp)          
        except:
          print str(traceback.format_exc())

def get_previous_residue(race_list):
  global model_name
  unique_ids=[]
  for t in race_list:
    ids={c.split("=")[0]:c.split("=")[1] for c in race_list[t].split("?")[1].split("&")}
    if ids['eventId'] not in unique_ids:
      unique_ids.append(ids['eventId'])          
  print len(unique_ids)
  r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
  if r.json()['hits']['hits']:
    last_date=datetime.datetime.utcfromtimestamp(int(sorted(r.json()['hits']['hits'],key=lambda x:x['_id'],reverse=True)[0]['_id'])/1000).strftime('%Y-%m-%d')
    tmp={}
    for t in sorted(r.json()['hits']['hits'],key=lambda x:x['_id'],reverse=True):
      dat=t['_source']
      if datetime.datetime.utcfromtimestamp(dat['timestamp']/1000).strftime('%Y-%m-%d')==last_date:
        if dat['event_id'] not in tmp:
          tmp[dat['event_id']]=[]
        if "result" in dat:
          tmp[dat['event_id']].append([dat['result'],dat['model_payout']])
  residue=0.0
  for t1 in tmp:
    for t2 in tmp[t1]:
      if t2[0]=="WON":
        break
      elif t2[0]=="LOST":
        residue=residue+t2[1]
        break
  return residue/float(len(unique_ids))

def model(dat,event_id,residue_amt):
  global r_s,min_stake
  highs=[]
  for t in dat:
    highs.append([sorted(t,key=lambda x:x['odds'])[0],t])
  model_match=None
  for t in sorted(highs,key=lambda x:x[0]['odds'])[::-1]:
    if t[0]['odds']>=1.33 and t[0]['odds']<=1.8:
      model_match=t[1]
      cur_runner=t[0]
      break
  if model_match:
    bet_amt=min_stake
    r1=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"query" : {"term" : { "event_id":event_id}}})
    if r1.json()['hits']['hits']:
      for t in sorted(r1.json()['hits']['hits'],key=lambda x:x['_id'],reverse=True):
        dat=t['_source']
        if dat['model_match']:
          if dat['result'] == "WON":
            bet_amt=min_stake
          elif dat['result'] == "LOST":
            bet_amt=round(dat['model_payout']/(cur_runner['odds']-1.0),2)
          elif dat['result'] == "VOID":
            bet_amt=dat['model_stake']
          elif dat['model_bet_id']:
            return None
          break
    else:
      if residue_amt:
        bet_amt=round(residue_amt/(cur_runner['odds']-1.0),2)
        if bet_amt<min_stake:
          bet_amt=min_stake+bet_amt
      else:
        bet_amt=min_stake
    bet_amt=round(bet_amt,2)
    player=cur_runner
    headerss={"Host":"www.betfair.com","Connection":"keep-alive","X-Requested-With":"XMLHttpRequest","BF-FP":bffp,"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36","Accept":"*/*","Accept-Encoding":"gzip, deflate, sdch, br","Accept-Language":"en-US,en;q=0.8"}
    r=r_s.get("https://www.betfair.com/sport/horse-racing?gaZone=Main&bseId="+player['bseId']+"&bsContext=REAL&bsmSt="+player['bsmSt']+"&bsUUID="+player['bsUUID']+"&gaMod=multipickavb&eventId="+player['eventId']+"&bssId="+player['bssId']+"&action=addSelection&bsmId="+player['bsmId']+"&modules=betslip&isSP=false&xsrftoken="+get_xsrf()+"&bsGroup="+player['bsGroup']+"&ts="+str(calendar.timegm(time.gmtime()))+"&alt=json",headers=headerss)
    bet_id=str(r.json()['page']['config']['instructions'][1]['arguments']['betRowId'].replace("#",""))
    html_source=r.json()['page']['config']['instructions'][1]['arguments']['event']
    soup = BeautifulSoup(html_source, 'html.parser') 
    max_val=str(soup.find_all(id=bet_id+"-maxStake")[0].get("value"))
    min_val=str(soup.find_all(id=bet_id+"-minStake")[0].get("value"))
    headerss={"Host":"www.betfair.com","Connection":"keep-alive","Content-Length":"917","Origin":"https://www.betfair.com","X-Requested-With":"XMLHttpRequest","BF-FP":bffp,"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36","Content-Type":"application/x-www-form-urlencoded","Accept":"*/*","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.8"}
    params={"timestamp":str(calendar.timegm(time.gmtime())),bet_id+"-inputStake":str(bet_amt),bet_id+"-stake":str(bet_amt),bet_id+"-odd":str(player['price']),bet_id+"-maxStake":max_val,bet_id+"-minStake":min_val,bet_id+"-tfoNumerator":str(player['num']),bet_id+"-tfoDenominator":str(player['den']),bet_id+"-dfoNumerator":str(player['num']),bet_id+"-dfoDenominator":str(player['den']),bet_id+"-bsIds":str(player['bsmId']).replace(".","_")+"-"+str(player['bssId']),bet_id+"-suspended":"false",bet_id+"-suspendedRunner":"false",bet_id+"-sequence":"0"}
    r=r_s.post("https://www.betfair.com/sport/place-bet?redirectPath=/football&action=place&modules=betslip&bsContext=REAL&xsrftoken="+get_xsrf(),data=params,headers=headerss)
    placed_bet_id=list(json_parse(r.json(),"parsedDataDimensions"))[0]["data-dimension9"]
    return {"model_match":model_match,"max_bet":max_val,"min_bet":min_val,"model_stake":bet_amt,"model_payout":bet_amt*cur_runner['odds'],"bet_id":placed_bet_id}
  return None

update_bffp()
update_params()
login()

race_list=get_list()
residue=get_previous_residue(race_list)
log("init","Success",None)

total_dat={}
while True:
  try:
    update_result()
  except:
    junk=1
  time.sleep(2)
  for t in sorted(race_list):
    cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
    if (cur_ts-t)>=-60 and (cur_ts-t)<=60 and t not in total_dat:
      print t
      try:
        dat=get_data(race_list[t])
        if dat:
          try:
            get_balance()
          except:
            login()
          final={}
          ids={c.split("=")[0]:c.split("=")[1] for c in race_list[t].split("?")[1].split("&")}
          final['event_id']=ids['eventId']
          tmp_dat=model(dat,ids['eventId'],residue)
          if tmp_dat:
            final['timestamp'] = t*1000
            final['race_url'] = race_list[t]
            final['model_match']=tmp_dat['model_match']
            final['model_max']=tmp_dat['max_bet']
            final['model_min']=tmp_dat['min_bet']
            final['model_stake']=tmp_dat['model_stake']
            final['model_bet_id']=tmp_dat['bet_id']
            final['model_payout']=tmp_dat['model_payout']
            for j in range(len(dat)):
              final['match_'+str(j+1)]=dat[j]
            r=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(final['timestamp']),json=final)
            print r.text
            total_dat[t]="added"
            log("new_race","Success",str(r.text))
          else:
            print "NOT temp_dat"
      except:
        log("new_race","Fail",str(traceback.format_exc()))




