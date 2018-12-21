import requests
import time
import xmltodict, json
import sys
from simplepush import send
from twilio.rest import Client  

movie_name="2"

def out(text):
 return json.loads(json.dumps(xmltodict.parse(text)))

def send_message():
  account_sid = 'ACca778ea0a07e4b4fd4f4b49f30c5874e' 
  auth_token = 'dbaa5742a34c95f6fee8f0b96360ae65' 
  client = Client(account_sid, auth_token) 
  message = client.messages.create(from_="whatsapp:+14155238886",body="Endiran 2.0 Ticket opened...",to="whatsapp:+918973605586")

while True:
  try:
    time.sleep(1)
    headers = {'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0.1; ONEPLUS A3003 Build/MMB29M)','Accept':'application/json','Host':'mweb.jazzcinemas.com','Accept-Encoding':'gzip'}
    r=requests.post("https://mweb.jazzcinemas.com/DataserviceJson.asmx/GetMovieList_ByType",headers=headers, data = {"lsCinemaId":"100","lsType":"ns"})
    movies=[]
    for i in out(r.text)['MovieInfoListByType']['MovieInfoItem']['MovieInfo']:
      movies.append(i['Movie_strName'])
    if [c for c in movies if movie_name in c]:
      print [c for c in movies if movie_name in c]
      send_message()
      send("4kKkF4", "Endiran 2.0","Endiran 2.0 Added...", "movies")
  except:
    print "-----"





