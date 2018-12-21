
import requests
r=requests.get("https://api.ipify.org")
new_ip=r.text.strip()
headers = {'X-Auth-Email': 'mohan.nehil@gmail.com','X-Auth-Key': '2b9fd4ecfe0669cfc9d252fa19db2be0d0631','Content-Type': 'application/json',}
r = requests.get('https://api.cloudflare.com/client/v4/zones/96d7a2672811b1aad2889806a24b8885/dns_records', headers=headers)
dns_records=[[c['id'],c['type'],c['name'],c['ttl'],c['proxied']] for c in r.json()['result']]
for t in dns_records:
 r = requests.put('https://api.cloudflare.com/client/v4/zones/96d7a2672811b1aad2889806a24b8885/dns_records/'+str(t[0]), headers=headers,json={"type":str(t[1]),"name":str(t[2]),"content":str(new_ip),"ttl":int(t[3]),"proxied":t[4]})



