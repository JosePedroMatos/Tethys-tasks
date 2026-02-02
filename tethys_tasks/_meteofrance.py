# from owslib.wcs import WebCoverageService
# import xmltodict
# import requests
# from osgeo import gdal
# from PIL import Image
# from io import BytesIO
# from os import remove , makedirs
# from time import sleep
# from datetime import datetime as dt, timedelta
# from os.path import join, exists
# import gc
# from tqdm import tqdm
 
# class MeteoFrance():
 
#     def __init__(self) -> None:
 
#         # self._token = "eyJ4NXQiOiJOelU0WTJJME9XRXhZVGt6WkdJM1kySTFaakZqWVRJeE4yUTNNalEyTkRRM09HRmtZalkzTURkbE9UZ3paakUxTURRNFltSTVPR1kyTURjMVkyWTBNdyIsImtpZCI6Ik56VTRZMkkwT1dFeFlUa3paR0kzWTJJMVpqRmpZVEl4TjJRM01qUTJORFEzT0dGa1lqWTNNRGRsT1RnelpqRTFNRFE0WW1JNU9HWTJNRGMxWTJZME13X1JTMjU2IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI5YTkxNTliMi0wZmU1LTRmZDAtYTRmMS1mMjI5ZGFjZWY2MDkiLCJhdXQiOiJBUFBMSUNBVElPTiIsImF1ZCI6InR0RkFXTVRSdk50TGVsQ2lhelVibHNHU3lQY2EiLCJuYmYiOjE3MDI4OTA5ODAsImF6cCI6InR0RkFXTVRSdk50TGVsQ2lhelVibHNHU3lQY2EiLCJzY29wZSI6ImRlZmF1bHQiLCJpc3MiOiJodHRwczpcL1wvcG9ydGFpbC1hcGkubWV0ZW9mcmFuY2UuZnJcL29hdXRoMlwvdG9rZW4iLCJleHAiOjE3Mzg4OTA5ODAsImlhdCI6MTcwMjg5MDk4MCwianRpIjoiYzU1MWFiM2MtZDcyNy00NTA4LWEyYzEtZGVkMDQwOWNlMTYxIiwiY2xpZW50X2lkIjoidHRGQVdNVFJ2TnRMZWxDaWF6VWJsc0dTeVBjYSJ9.G7XG1qEbKQz2LbKIYb1gWCU4wJKj72EgaN1yGVleMZQe9y_jVUHxtSYq_ZT7R5rgSGnB4cYdESrlbDdZtKPK5MWQZn8A4mZQPjHat0MRATRQt_PQiZ8731p3OtbrK_O2fkOAb-yW7GWXxKoa9WwggGjR7G2HCPi61DZcA_cbZ00K672T9V92xyseK5b7zVgfEVQoQGOOX6RZ03pqbUdf4dYG_PtTRfnKQB30XwMuVePPYQ6IW01MM9vW0vhA5HXPx7ve31_bsNA_qtzi6rdz88-IVYpGjF1Mce9WzLoKhdOR4goyT8w1seSakAy5LwqICkT-vc11KvycmyaXTKCLpQ"
#         self._token = "eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJQaWVycmVBcmNoYW1iZWF1QGNhcmJvbi5zdXBlciIsImFwcGxpY2F0aW9uIjp7Im93bmVyIjoiUGllcnJlQXJjaGFtYmVhdSIsInRpZXJRdW90YVR5cGUiOm51bGwsInRpZXIiOiJVbmxpbWl0ZWQiLCJuYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaWQiOjU1NTYsInV1aWQiOiJkN2UzNmZjNC01NGQ3LTRlYjMtOGNmZS1lMjg4YmRlNDA2MTgifSwiaXNzIjoiaHR0cHM6XC9cL3BvcnRhaWwtYXBpLm1ldGVvZnJhbmNlLmZyOjQ0M1wvb2F1dGgyXC90b2tlbiIsInRpZXJJbmZvIjp7IjUwUGVyTWluIjp7InRpZXJRdW90YVR5cGUiOiJyZXF1ZXN0Q291bnQiLCJncmFwaFFMTWF4Q29tcGxleGl0eSI6MCwiZ3JhcGhRTE1heERlcHRoIjowLCJzdG9wT25RdW90YVJlYWNoIjp0cnVlLCJzcGlrZUFycmVzdExpbWl0IjowLCJzcGlrZUFycmVzdFVuaXQiOiJzZWMifX0sImtleXR5cGUiOiJQUk9EVUNUSU9OIiwic3Vic2NyaWJlZEFQSXMiOlt7InN1YnNjcmliZXJUZW5hbnREb21haW4iOiJjYXJib24uc3VwZXIiLCJuYW1lIjoiQVJPTUUiLCJjb250ZXh0IjoiXC9wdWJsaWNcL2Fyb21lXC8xLjAiLCJwdWJsaXNoZXIiOiJhZG1pbl9tZiIsInZlcnNpb24iOiIxLjAiLCJzdWJzY3JpcHRpb25UaWVyIjoiNTBQZXJNaW4ifSx7InN1YnNjcmliZXJUZW5hbnREb21haW4iOiJjYXJib24uc3VwZXIiLCJuYW1lIjoiQVJQRUdFIiwiY29udGV4dCI6IlwvcHVibGljXC9hcnBlZ2VcLzEuMCIsInB1Ymxpc2hlciI6ImFkbWluX21mIiwidmVyc2lvbiI6IjEuMCIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxODE3Nzc0NTkyLCJ0b2tlbl90eXBlIjoiYXBpS2V5IiwiaWF0IjoxNzIzMTAxNzkyLCJqdGkiOiI2YWJmZjM2Yy0yOTE0LTRjMDMtYWY4ZC1lNDhmOGJjM2RkYzUifQ==.npmNfoENOoXaeHsjFhLKELhkxlaYG1q04Z8whdHDqFR-MeqnOx2H4ErdWbeJvbzNCCXfY8VoUWruz-OawrDt7upMyzMWSY52wTCtuSOj0ikr-MHY0Vxo8Uk2NSX1MnCPd48Sg0UL-jS0nuI9-KHtSmn8oE_p47CQTmu7BeCi-osl4lu322Tx9Km3Z2Nc3YcMnaTrxuO0fOLTcK5PBMdCcdsNLF2qmu_1YgASU2GQ_W5RSm1UV9Ya_XSVlnZzndGu-oeKvQE5eKvgz5JqxdCmUqJrLXb6F_8abhmj27PwshqZttQjP9l2FRaHT3pLPAWATwSUj4yD076ZM92w4WCU-g=="
 
#         self._headers  = {'apikey': '{}'.format(self._token),
#         # self._headers  = {'Authorization': 'Bearer {}'.format(self._token),
#                          'accept' : '*/*'}
 
#         self._arome = requests.get('https://public-api.meteofrance.fr/public/arome/1.0/wcs/MF-NWP-HIGHRES-AROME-0025-FRANCE-WCS/GetCapabilities?service=WCS&version=2.0.1&language=fre&format=json%27,
#                             verify=True,
#                             headers=self._headers)
 
#         self.capabilities = xmltodict.parse(self._arome.text)
#         self.dates = self._list_reference_times()
 
#     def _list_reference_times(self):
 
#         contents:list
#         contents = self.capabilities['wcs:Capabilities']['wcs:Contents']['wcs:CoverageSummary']
 
#         titles = [curcont['ows:Title'] for curcont in contents]
#         coverageid = [curcont['wcs:CoverageId'] for curcont in contents]
 
#         available = [curcov for curcov in coverageid if 'TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE' in curcov]
#         dates = [dt.strptime(curcov.split('___')[1].split('_')[0], '%Y-%m-%dT%H.%M.%SZ') for curcov in available]
 
#         return list(set(dates))
#         pass
 
#     def _get_one_prevision(self,date_prev:dt, date_cible:dt, dirout:str):
 
#         fileout = join(dirout,'{}_{}.tif'.format(date_prev.strftime('%Y%m%d_%H'), date_cible.strftime('%Y%m%d_%H')))
 
#         if exists(fileout):
#             return
 
#         #TOTAL_WATER_PRECIPITATION__GROUND_OR_WATER_SURFACE
#         #TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE
 
#         coverageid = 'TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE___' \
#                     + date_prev.strftime('%Y-%m-%dT%H:%M:%SZ_PT9H') #2023-10-11T09:00:00Z_PT9H'
#         subset_time ='time({})'.format(date_cible.strftime('%Y-%m-%dT%H:%M:%SZ')) #'time(2023-10-13T11:00:00Z)'
 
#         subset_lat = 'lat(47.,53.)'
#         subset_lon = 'long(2.,10.)'
 
#         img = requests.get('https://public-api.meteofrance.fr/public/arome/1.0/wcs/MF-NWP-HIGHRES-AROME-0025-FRANCE-WCS/GetCoverage?service=WCS&version=2.0.1&format=image/tiff&coverageid={}&subset={}&subset={}&subset={}'.format(coverageid, subset_time, subset_lat, subset_lon),
#                             verify=True,
#                             headers=self._headers)
 
#         if 'NoSuchCoverage' in img.text or 'Synopsis backend error' in img.text:
#             return
 
#         if 'You have exceeded your quota' in img.text:
#             sleep(1.)
#             img = requests.get('https://public-api.meteofrance.fr/public/arome/1.0/wcs/MF-NWP-HIGHRES-AROME-0025-FRANCE-WCS/GetCoverage?service=WCS&version=2.0.1&format=image/tiff&coverageid={}&subset={}&subset={}&subset={}'.format(coverageid, subset_time, subset_lat, subset_lon),
#                                 verify=True,
#                                 headers=self._headers)
 
#         out = open('tmp_meteofrance.tif', 'wb')
#         out.write(img.content)
#         out.close()
 
#         tmptif = gdal.Open('tmp_meteofrance.tif')
 
#         warpoptions = gdal.WarpOptions(gdal.ParseCommandLine("-t_srs EPSG:31370 -of vrt"))
#         tmpvrt = gdal.Warp('tmp_meteofrance.vrt', tmptif, options= warpoptions )
 
#         translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -co COMPRESS=LZW"))
#         out = gdal.Translate(fileout, tmpvrt, options=translateoptions)
 
#         tmptif = None
#         tmpvrt = None
 
#         gc.collect() #Force la fermeture des fichiers pour pouvoir les supprimer proprement
 
#         remove('tmp_meteofrance.tif')
#         remove('tmp_meteofrance.vrt')
 
#     def get_previsions(self, date_prev:dt, dirbase:str):
 
#         dirout = join(dirbase, date_prev.strftime('%Y%m%d_%H'))
 
#         if exists(dirout):
#             return
 
#         makedirs(dirout, exist_ok=True)
 
#         start_req = dt.now()
 
#         for curhour in tqdm(range(9,52), dirout):
#             date_cible = date_prev +timedelta(0,seconds=curhour*3600)
#             self._get_one_prevision(date_prev, date_cible, dirout=dirout)
 
#         end_req = dt.now()
 
#         sleep(61 - (end_req-start_req).seconds) #On ne peut faire que 50 requêtes à la minute --> on patiente !
 
# if __name__=='__main__':
 
#     import numpy as np
 
#     dirbase=r'D:\OneDrive\Universite de Liege\HECE - Data\Pluies\MeteoFrance\AROME'
 
#     now = dt.now()
 
#     hour = now.hour - np.mod(now.hour,3)
 
#     arome = MeteoFrance()
 
#     for date_prev in arome.dates:
#         arome.get_previsions(date_prev, dirbase)
 