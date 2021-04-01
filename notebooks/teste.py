import pandas as pd
import numpy as np
import datetime as dt
import sys
from user_agents import parse
from geolite2 import geolite2
path = sys.argv[1]
saida = sys.argv[2]
puro = pd.read_fwf(path, header=None)
array = puro.loc[(puro[1].isnull())][0]
puro = pd.DataFrame({"linha":array })
dados = puro[~puro["linha"].str.contains("#")].copy()
dados = pd.DataFrame(dados.linha.str.split(' ').tolist())
dados['data_hora'] = pd.to_datetime(dados[0] + ' ' + dados[1])
dados['data_hora'] = dados['data_hora'] + dt.timedelta(hours = -3)
dados['sevidor_nm_site_iis'] = dados[2]
dados['servidor_nm'] = dados[3]
dados['servidor_ip'] = dados[4]
dados['requisicao_method'] = dados[5]
dados['requisicao_recurso'] = dados[6]
dados['requisicao_query'] = dados[7]
dados['requisicao_port'] = pd.to_numeric(dados[8])
dados['requisicao_usuario'] = dados[9]
dados['cliente_ip'] = dados[10]
dados['requisicao_protocolo'] = dados[11]
dados['cliente_user_agent'] = dados[12]
dados['requisicao_url_origem'] = dados[14]
dados['servidor_nm_site'] = dados[15]
dados['requisicao_cod_resposta'] = pd.to_numeric(dados[16])
dados['requisicao_dados_enviados'] = pd.to_numeric(dados[19])
dados['requisicao_dados_recebidos'] = pd.to_numeric(dados[20])
dados['requisicao_tempo_resposta'] = pd.to_numeric(dados[21])
dados['arquivo'] = path
dados = dados.drop([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21], axis=1).copy()
nm_agents = dados.cliente_user_agent.unique()
browser_family = []
browser_version = []
os_family = []
os_version = []
device_family = []
device_model = []
device_brand = []
for x in nm_agents:
  user_agent = parse(x)
  browser_family.append(user_agent.browser.family)
  browser_version.append(user_agent.browser.version_string)
  os_family.append(user_agent.os.family)
  os_version.append(user_agent.os.version_string)
  device_family.append(user_agent.device.family)
  device_model.append(user_agent.device.model)
  device_brand.append(user_agent.device.brand)
dispositivo = pd.DataFrame(
    {"cliente_user_agent":nm_agents, 
     "os_family": os_family, 
     "os_version": os_version, 
     "device_family": device_family, 
     "device_model": device_model, 
     "device_brand": device_brand, 
     "browser_family": browser_family,
     "browser_version": browser_version     
     })
dados =  pd.merge(dados, dispositivo , sort=True, copy=True, on="cliente_user_agent")
dados = dados.drop(['cliente_user_agent'], axis=1).copy()
ip_clients = dados.cliente_ip.unique()
def get_country(ip):
    try:
        x = geo.get(ip)
    except ValueError:
        return np.nan
    try:
        return x['country']['names']['en'] if x else np.nan
    except KeyError:
        return np.nan

geo = geolite2.reader()

pais_ip =  pd.DataFrame({"cliente_ip":ip_clients}) 
# get unique IPs
unique_ips = pais_ip['cliente_ip'].unique()
# make series out of it
unique_ips = pd.Series(unique_ips, index = unique_ips)
# map IP --> country
pais_ip['cliente_pais'] = pais_ip['cliente_ip'].map(unique_ips.apply(get_country))

geolite2.close()
dados =  pd.merge(dados, pais_ip , sort=True, copy=True, on="cliente_ip")
dados.to_csv(saida , index=False)
