import requests
import grequests
import traceback
import time
from pprint import pprint
from bs4 import BeautifulSoup
import dateutil.parser
from datetime import timedelta
import datetime
import pytz
import pickle

def get_params():
  r=requests.get("https://www.betfair.com/exchange/plus/")
  ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()
  return [ak]

[ak]=get_params()

def get_aus_states(refresh=0):
  if refresh:
    r=requests.get("http://www.skyracingworld.com/track-abbreviations")
    soup=BeautifulSoup(r.text,"html.parser")
    states=[]
    for t in soup.find_all("td",width=True):
      if t['width']=='37':
        if t.text.strip() not in states:
          states.append(t.text.strip())
    return states
  else:
    return ['NSW','NT','WA','QLD','VIC','SA','ACT','TAS']

def get_meetings_aus():
  race_date=time.strftime("%Y-%m-%d")
  r=requests.get("http://www.skyracing.com.au/ajax.php?action=gettodayraces&Itemid=89&sEcho=2&iColumns=10&sColumns=&iDisplayStart=0&iDisplayLength=-1&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&mDataProp_4=4&mDataProp_5=5&mDataProp_6=6&mDataProp_7=7&mDataProp_8=8&mDataProp_9=9&iSortingCols=0&bSortable_0=false&bSortable_1=false&bSortable_2=false&bSortable_3=false&bSortable_4=false&bSortable_5=false&bSortable_6=false&bSortable_7=false&bSortable_8=false&bSortable_9=false&racedate="+str(race_date)+"&racetype=R")
  race_list=[]
  for t in r.json()['aaData']:
    if BeautifulSoup(t[1],"html.parser").text.strip() in get_aus_states():
      race_list.append("http://www.skyracing.com.au/"+BeautifulSoup(t[1],"html.parser").find("a").get("href"))
  return race_list

def get_meetings_uk():
  race_date=time.strftime("%Y-%m-%d")
  r=requests.get("http://www.skyracing.com.au/ajax.php?action=gettodayraces&Itemid=89&sEcho=2&iColumns=10&sColumns=&iDisplayStart=0&iDisplayLength=-1&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&mDataProp_4=4&mDataProp_5=5&mDataProp_6=6&mDataProp_7=7&mDataProp_8=8&mDataProp_9=9&iSortingCols=0&bSortable_0=false&bSortable_1=false&bSortable_2=false&bSortable_3=false&bSortable_4=false&bSortable_5=false&bSortable_6=false&bSortable_7=false&bSortable_8=false&bSortable_9=false&racedate="+str(race_date)+"&racetype=R")
  race_list=[]
  for t in r.json()['aaData']:
    if BeautifulSoup(t[1],"html.parser").text.strip() in ["UK","IR"]:
      race_list.append("http://www.skyracing.com.au/"+BeautifulSoup(t[1],"html.parser").find("a").get("href"))
  return race_list

def get_races_aus():
  race_lists={}
  for cur_url in get_meetings_aus():
    repeats=[]
    r=requests.get(cur_url)
    soup=BeautifulSoup(r.text,"html.parser")
    for t in soup.find_all("table")[-1].find("tbody").find_all("tr"):
      race_url="http://www.skyracing.com.au/"+t.find_all("td")[0].find("a").get("href")
      australia = pytz.timezone('Australia/Sydney')
      pytz.utc = pytz.timezone('UTC')
      loc_dt = australia.localize(dateutil.parser.parse(time.strftime("%Y-%m-%d")+"T"+t.find_all("td")[2].text.strip()+":00.000Z").replace(tzinfo=None))
      loc_dt = australia.normalize(loc_dt)
      race_time = int(time.mktime(loc_dt.astimezone(pytz.utc).timetuple()))
      if race_time not in race_lists:
        race_lists[race_time]=race_url
      else:
        repeats.append(race_time)
  for t in repeats:
    del repeats[t]
  return race_lists

def get_skydata(cur_url):
  r=requests.get(cur_url)
  soup=BeautifulSoup(r.text,"html.parser")
  ratings_manual=[[int(c1.strip().replace("*","")) for c1 in c.text.strip().split("-")] for c in soup.find(id="skyFormDiv").find_all(class_="theTips")]
  ratings_auto=[[] for i in range(8)]
  for c1 in soup.find(id="paneldetails").find_all("tr")[1:5]:
    i=0
    for c2 in c1.find_all("td"):
      try:
        ratings_auto[i].append(int(c2.text.split(" ")[0].strip()))
      except:
        junk=1
      i=i+1
  for c1 in soup.find(id="paneldetails").find_all("tr")[5:]:
    i=4
    for c2 in c1.find_all("td"):
      try:
        ratings_auto[i].append(int(c2.text.split(" ")[0].strip()))
      except:
        junk=1
      i=i+1
  horse_details=[]
  for t in soup.find_all("tr"):
    if t.find(class_="expandTD"):
      hr_num=int("".join([c for c in t.find_all("td")[2].text.strip() if c.isdigit()]))
      hr_form=t.find_all("td")[3].text.strip()
      hr_name=t.find_all("td")[4].text.strip()
      rate1=int(t.find_all("td")[10].text.strip())
      rate2=int(t.find_all("td")[11].text.strip())
      horse_details.append({"rating_2":rate2,"rating_1":rate1,"horse_form":hr_form,"horse_number":hr_num,"horse_name":hr_name})
  return {"horse_details":horse_details,"ratings_auto":ratings_auto,"ratings_manual":ratings_manual}

def betfair_list_uk():
  global ak
  r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[7],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
  r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId=7&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
  race_list={}
  repeats=[]
  for t in r.json()['races']:
    if "(" not in t['meetingName']:
      eastern = pytz.timezone('Europe/London')
      pytz.utc = pytz.timezone('UTC')
      loc_dt = eastern.localize(dateutil.parser.parse(t['startTime']).replace(tzinfo=None))
      loc_dt = eastern.normalize(loc_dt)
      race_time = int(time.mktime(loc_dt.astimezone(pytz.utc).timetuple()))
      if race_time in race_list:
        repeats.append(race_time)
      else:
        race_list[race_time]=t['raceId']
  for t in repeats:
    try:
      del race_list[t]
    except:
      junk=1
  return race_list

def betfair_list_aus():
  global ak
  r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[7],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
  r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId=7&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
  race_list={}
  repeats=[]
  for t in r.json()['races']:
    if "AUS" in t['meetingName'] or "NZL" in t['meetingName']:
      eastern = pytz.timezone('Europe/London')
      pytz.utc = pytz.timezone('UTC')
      loc_dt = eastern.localize(dateutil.parser.parse(t['startTime']).replace(tzinfo=None))
      loc_dt = eastern.normalize(loc_dt)
      race_time = int(time.mktime(loc_dt.astimezone(pytz.utc).timetuple()))
      if race_time in race_list:
        repeats.append(race_time)
      else:
        race_list[race_time]=t['raceId']
  for t in repeats:
    del race_list[t]
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
  r=requests.get("https://strands.betfair.com/api/eds/racing-info/v1?_ak=nzIFcwyWhrlwYMrh&marketId="+str(dat['WIN']['marketId']))
  fin_out={}
  for t in r.json()['horseDetails']:
    hr_dat={"win_lay":None,"win_back":None,"place_lay":None,"place_back":None}
    hr_dat['last_ran']=r.json()['horseDetails'][t]['daysSinceLastRun'] if "daysSinceLastRun" in r.json()['horseDetails'][t] else 0
    hr_dat['horse_name']=r.json()['horseDetails'][t]['name']
    hr_dat['horse_number']=int(r.json()['horseDetails'][t]['saddleCloth'])
    fin_out[int(r.json()['horseDetails'][t]['saddleCloth'])]=hr_dat
  for t2 in dat['WIN']['marketData']:
    fin_out[t2['horse_number']]["win_lay"]=t2['layPrice']
    fin_out[t2['horse_number']]["win_back"]=t2['backPrice']
  for t2 in dat['PLACE']['marketData']:
    fin_out[t2['horse_number']]["place_lay"]=t2['layPrice']
    fin_out[t2['horse_number']]["place_back"]=t2['backPrice']
  market_ids={}
  for t in dat.keys():
    market_ids[t]=dat[t]['marketId']
  return [fin_out,market_ids]

def get_races_uk():
  race_lists={}
  for cur_url in get_meetings_uk():
    repeats=[]
    r=requests.get(cur_url)
    soup=BeautifulSoup(r.text,"html.parser")
    for t in soup.find_all("table")[-1].find("tbody").find_all("tr"):
      race_url="http://www.skyracing.com.au/"+t.find_all("td")[0].find("a").get("href")
      race_time=t.find_all("td")[2].text.strip()
      time_hr=int(race_time.split(":")[0])
      if time_hr<=12:
        race_time=datetime.datetime.strftime(datetime.datetime.today() +datetime.timedelta(1),'%Y-%m-%d')+"T"+race_time+":00.000Z"
      else:
        race_time=time.strftime("%Y-%m-%d")+"T"+race_time+":00.000Z"
      australia = pytz.timezone('Australia/Sydney')
      pytz.utc = pytz.timezone('UTC')
      loc_dt = australia.localize(dateutil.parser.parse(race_time).replace(tzinfo=None))
      loc_dt = australia.normalize(loc_dt)
      race_time = int(time.mktime(loc_dt.astimezone(pytz.utc).timetuple()))
      if race_time not in race_lists:
        race_lists[race_time]=race_url
      else:
        repeats.append(race_time)
    for t in repeats:
      del repeats[t]
  return race_lists

try:
  try:
    initial_backup=pickle.load(open(time.strftime("%Y-%m-%d")+"_sky_backup","rb"))
  except:
    uk_sky_list = get_races_uk()
    uk_betfair_list = betfair_list_uk()
    common_list=[c for c in uk_sky_list if c in uk_betfair_list]
    initial_backup={}
    for t in common_list:
      print t
      dat2=get_skydata(uk_sky_list[t])
      initial_backup[t]={"dat2":dat2,"betfair_raceid":uk_betfair_list[t],"skyracing_url":uk_sky_list[t]}
    pickle.dump(initial_backup,open(time.strftime("%Y-%m-%d")+"_sky_backup","wb"))
except:
  junk=1

print len(initial_backup)
total_dat={}
model_name="skyracing"
while True:
  time.sleep(2)
  for t in sorted(initial_backup):
    cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
    if cur_ts>=t-180 and cur_ts<=t+120 and t not in total_dat:
      try:
        dat1=betfair_dat(initial_backup[t]['betfair_raceid'])
        dat2=initial_backup[t]['dat2']
        final={}
        final['timestamp']=t*1000
        final['skyracing_url']=initial_backup[t]['skyracing_url']
        final['ratings_auto']=dat2['ratings_auto']
        final['ratings_manual']=dat2['ratings_manual']
        final['betfair_place_id']=dat1[1]['PLACE'] if "PLACE" in dat1[1] else None
        final['betfair_win_id']=dat1[1]['WIN'] if "WIN" in dat1[1] else None
        final['horse_data']=[]
        for t1 in dat2['horse_details']:
          if t1['horse_number'] in dat1[0]:
            flag=1
            for t2 in dat1[0][t1['horse_number']]:
              t1[t2]=dat1[0][t1['horse_number']][t2]
              if t2=="win_back" and not dat1[0][t1['horse_number']][t2]:
                flag=0
            if flag:
              final['horse_data'].append(t1)
        r=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(final['timestamp']),json=final)
        total_dat[t]="added"
      except:
        print "-----"





