#Le code pourrait être plus concis, néanmoins cette forme "développée" facilite les modifications nécessaires dues aux changements de périmètres et de méthodes de calcul en fonction des années.
### Importations 

import pandas as pd 
import pyarrow.parquet as pq
import numpy as np
import pyreadstat
from functools import reduce
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import seaborn as sns

### Variables d'environnement 

#Dictionnaires de liens entre les nomenclatures
regions = {"10":"11","B0":"24","C1":"27","C2":"27","D1":"28","D2":"28","E1":"32","E2":"32","F1":"44","F2":"44","F3":"44","G0":"52","H0":"53","I1":"75","I2":"75","I3":"75","J1":"76","J2":"76","K1":"84","K2":"84","L0":"93","M0":"94","Y1":"101","Y2":"102","Y3":"103","Y4":"104"}
regionsav = {'01':'101','02':'102','03':'103','04':'104','11':'11','24':'24','27':'27','28':'28','32':'32','44':'44','52':'52','53':'53','75':'75','76':'76','84':'84','93':'93','94':'94'}
regionsavap = {'1':'101','2':'102','3':'103','4':'104','11':'11','24':'24','27':'27','28':'28','32':'32','44':'44','52':'52','53':'53','75':'75','76':'76','84':'84','93':'93','94':'94'}
regionsavapEEC = {'01':'101','02':'102','03':'103','04':'104','11':'11','21':'44','22':'32','23':'28','25':'28','24':'24','26':'27','31':'32','41':'44','42':'44','43':'27','52':'52','53':'53','54':'75','72':'75','73':'76','74':'75','82':'84','83':'84','91':'76','93':'93','94':'94'}

#Variable indicatrice de formation professionelle dans les EEC 2021 et postérieurs
fnf = 'FNFPROM'

### Données

#df101, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2010/birepsas_basej101.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df102, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2010/birepsas_basej102.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df103, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2010/birepsas_basej103.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df104, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2010/birepsas_basej104.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df111, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2011/birepsas_basej111.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df112, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2011/birepsas_basej112.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df113, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2011/birepsas_basej113.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df114, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2011/birepsas_basej114.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df121, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2012/birepsas_basej121.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df122, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2012/birepsas_basej122.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df123, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2012/birepsas_basej123.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
#df124, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2012/birepsas_basej124.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df131, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2013/birepsas_basez131.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df132, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2013/birepsas_basez132.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df133, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2013/birepsas_basez133.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df134, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2013/birepsas_basez134.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df141, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2014/t141z.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df142, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2014/t142z.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df143, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2014/t143z.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df144, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2014/t144z.sas7bdat", usecols=["REG","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df151, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2015/t151z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df152, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2015/t152z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df153, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2015/t153z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df154, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2015/t154z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df161, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2016/t161z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df162, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2016/t162z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df163, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2016/t163z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df164, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2016/t164z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df171, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2017/t171z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df172, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2017/t172z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df173, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2017/t173z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df174, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2017/t174z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df181, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2018/t181z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df182, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2018/t182z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df183, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2018/t183z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df184, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2018/t184z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df191, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2019/t191z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df192, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2019/t192z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df193, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2019/t193z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df194, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2019/t194z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df201, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2020/t201z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df202, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2020/t202z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df203, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2020/t203z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df204, meta=pyreadstat.read_sas7bdat("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2020/t204z.sas7bdat", usecols=["REGIO","LFS_REGIONW","SIRET","FORTYP","FC5D","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df211=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2021/t211z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df212=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2021/t212z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df213=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2021/t213z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df214=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2021/t214z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df221=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2022/t221z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df222=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2022/t222z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df223=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2022/t223z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df224=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2022/t224z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df231=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2023/t231z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df232=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2023/t232z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])
df233=pd.read_parquet("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_2023/t233z.parquet", columns=["LFS_REGION","LFS_REGIONW","SIRETRF","FNFPROM","FNFLOISM","FNFPROA_Y","FFA_CPF_Y","FFALT","FNFAFEST_Y","FNFCERT_Y","FNFCPF_Y","FNFLOISA_Y","FNFRDUR_Y","NAFG010N"])

#df101['an'] = 10
#df101['trim'] = 1
#df102['an'] = 10
#df102['trim'] = 2
#df103['an'] = 10
#df103['trim'] = 3
#df104['an'] = 10
#df104['trim'] = 4
#df111['an'] = 11
#df111['trim'] = 1
#df112['an'] = 11
#df112['trim'] = 2
#df113['an'] = 11
#df113['trim'] = 3
#df114['an'] = 11
#df114['trim'] = 4
#df121['an'] = 12
#df121['trim'] = 1
#df122['an'] = 12
#df122['trim'] = 2
#df123['an'] = 12
#df123['trim'] = 3
#df124['an'] = 12
#df124['trim'] = 4
df131['an'] = 13
df131['trim'] = 1
df132['an'] = 13
df132['trim'] = 2
df133['an'] = 13
df133['trim'] = 3
df134['an'] = 13
df134['trim'] = 4
df141['an'] = 14
df141['trim'] = 1
df142['an'] = 14
df142['trim'] = 2
df143['an'] = 14
df143['trim'] = 3
df144['an'] = 14
df144['trim'] = 4
df151['an'] = 15
df151['trim'] = 1
df152['an'] = 15
df152['trim'] = 2
df153['an'] = 15
df153['trim'] = 3
df154['an'] = 15
df154['trim'] = 4
df161['an'] = 16
df161['trim'] = 1
df162['an'] = 16
df162['trim'] = 2
df163['an'] = 16
df163['trim'] = 3
df164['an'] = 16
df164['trim'] = 4
df171['an'] = 17
df171['trim'] = 1
df172['an'] = 17
df172['trim'] = 2
df173['an'] = 17
df173['trim'] = 3
df174['an'] = 17
df174['trim'] = 4
df181['an'] = 18
df181['trim'] = 1
df182['an'] = 18
df182['trim'] = 2
df183['an'] = 18
df183['trim'] = 3
df184['an'] = 18
df184['trim'] = 4
df191['an'] = 19
df191['trim'] = 1
df192['an'] = 19
df192['trim'] = 2
df193['an'] = 19
df193['trim'] = 3
df194['an'] = 19
df194['trim'] = 4
df201['an'] = 20
df201['trim'] = 1
df202['an'] = 20
df202['trim'] = 2
df203['an'] = 20
df203['trim'] = 3
df204['an'] = 20
df204['trim'] = 4
df211['an'] = 21
df211['trim'] = 1
df212['an'] = 21
df212['trim'] = 2
df213['an'] = 21
df213['trim'] = 3
df214['an'] = 21
df214['trim'] = 4
df221['an'] = 22
df221['trim'] = 1
df222['an'] = 22
df222['trim'] = 2
df223['an'] = 22
df223['trim'] = 3
df224['an'] = 22
df224['trim'] = 4
df231['an'] = 23
df231['trim'] = 1
df232['an'] = 23
df232['trim'] = 2
df233['an'] = 23
df233['trim'] = 3

#df11 = pd.concat([df111[['REG','FC5D','trim']],df112[['REG','FC5D','trim']],df113[['REG','FC5D','trim']],df114[['REG','FC5D','trim']]],ignore_index=True)
#df12 = pd.concat([df121[['REG','FC5D','trim']],df122[['REG','FC5D','trim']],df123[['REG','FC5D','trim']],df124[['REG','FC5D','trim']]],ignore_index=True)
df13 = pd.concat([df131[['REG','FC5D',"NAFG010N",'trim']],df132[['REG','FC5D',"NAFG010N",'trim']],df133[['REG','FC5D',"NAFG010N",'trim']],df134[['REG','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df14 = pd.concat([df141[['REG','FC5D',"NAFG010N",'trim']],df142[['REG','FC5D',"NAFG010N",'trim']],df143[['REG','FC5D',"NAFG010N",'trim']],df144[['REG','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df15 = pd.concat([df151[['REGIO','FC5D',"NAFG010N",'trim']],df152[['REGIO','FC5D',"NAFG010N",'trim']],df153[['REGIO','FC5D',"NAFG010N",'trim']],df154[['REGIO','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df16 = pd.concat([df161[['REGIO','FC5D',"NAFG010N",'trim']],df162[['REGIO','FC5D',"NAFG010N",'trim']],df163[['REGIO','FC5D',"NAFG010N",'trim']],df164[['REGIO','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df17 = pd.concat([df171[['REGIO','FC5D',"NAFG010N",'trim']],df172[['REGIO','FC5D',"NAFG010N",'trim']],df173[['REGIO','FC5D',"NAFG010N",'trim']],df174[['REGIO','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df18 = pd.concat([df181[['REGIO','FC5D',"NAFG010N",'trim']],df182[['REGIO','FC5D',"NAFG010N",'trim']],df183[['REGIO','FC5D',"NAFG010N",'trim']],df184[['REGIO','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df19 = pd.concat([df191[['REGIO','FC5D',"NAFG010N",'trim']],df192[['REGIO','FC5D',"NAFG010N",'trim']],df193[['REGIO','FC5D',"NAFG010N",'trim']],df194[['REGIO','FC5D',"NAFG010N",'trim']]],ignore_index=True)
df20 = pd.concat([df201[['REGIO','FC5D',"NAFG010N",'trim']],df202[['REGIO','FC5D',"NAFG010N",'trim']],df203[['REGIO','FC5D',"NAFG010N",'trim']],df204[['REGIO','FC5D',"NAFG010N",'trim']]],ignore_index=True)
#df11['REGIO'] = df11['REG'].apply(lambda x : str(x)[:2]).map(regionsavapEEC)
#df12['REGIO'] = df12['REG'].apply(lambda x : str(x)[:2]).map(regionsavapEEC)
df13['REGIO'] = df13['REG'].apply(lambda x : str(x)[:2]).map(regionsavapEEC)
df14['REGIO'] = df14['REG'].apply(lambda x : str(x)[:2]).map(regionsavapEEC)
df15['REGIO'] = df15['REGIO'].apply(lambda x : str(x)[:2]).map(regionsav)
df16['REGIO'] = df16['REGIO'].apply(lambda x : str(x)[:2]).map(regionsav)
df17['REGIO'] = df17['REGIO'].apply(lambda x : str(x)[:2]).map(regionsav)
df18['REGIO'] = df18['REGIO'].apply(lambda x : str(x)[:2]).map(regionsav)
df19['REGIO'] = df19['REGIO'].apply(lambda x : str(x)[:2]).map(regionsav)
df20['REGIO'] = df20['REGIO'].apply(lambda x : str(x)[:2]).map(regionsav)
#df11 = df11.dropna(subset=['REGIO'])
#df12 = df12.dropna(subset=['REGIO'])
df13 = df13.dropna(subset=['REGIO'])
df14 = df14.dropna(subset=['REGIO'])
df15 = df15.dropna(subset=['REGIO'])
df16 = df16.dropna(subset=['REGIO'])
df17 = df17.dropna(subset=['REGIO'])
df18 = df18.dropna(subset=['REGIO'])
df19 = df19.dropna(subset=['REGIO'])
df20 = df20.dropna(subset=['REGIO'])
df21 = pd.concat([df211[['LFS_REGION',fnf,"NAFG010N",'trim']],df212[['LFS_REGION',fnf,"NAFG010N",'trim']],df213[['LFS_REGION',fnf,"NAFG010N",'trim']],df214[['LFS_REGION',fnf,"NAFG010N",'trim']]],ignore_index=True)
df22 = pd.concat([df221[['LFS_REGION',fnf,"NAFG010N",'trim']],df222[['LFS_REGION',fnf,"NAFG010N",'trim']],df223[['LFS_REGION',fnf,"NAFG010N",'trim']],df224[['LFS_REGION',fnf,"NAFG010N",'trim']]],ignore_index=True)
df23 = pd.concat([df231[['LFS_REGION',fnf,"NAFG010N",'trim']],df232[['LFS_REGION',fnf,"NAFG010N",'trim']],df233[['LFS_REGION',fnf,"NAFG010N",'trim']]],ignore_index=True)
df21['LFS_REGION'] = df21['LFS_REGION'].apply(lambda x : str(x)[:2]).map(regions)
df22['LFS_REGION'] = df22['LFS_REGION'].apply(lambda x : str(x)[:2]).map(regions)
df23['LFS_REGION'] = df23['LFS_REGION'].apply(lambda x : str(x)[:2]).map(regions)
df21 = df21.dropna(subset=['LFS_REGION'])
df22 = df22.dropna(subset=['LFS_REGION'])
df23 = df23.dropna(subset=['LFS_REGION'])

### Choix du modèle :

#Choix du modèle executé :

# 1 : Variable explicatives : croissance des dépenses et année
# 2 : Variable explicatives : croissance des dépenses, secteur professionnel et année
# 3 : Variable explicatives : croissance des dépenses et année, corrigé de l'IPC
# 4 : Variable explicatives : croissance des dépenses, secteur professionnel et année, corrigé de l'IPC (non présenté dans le rapport)
# 5 : Variable explicatives : croissance des dépenses (n et n-1) et année
# 6 : Variable explicatives : croissance des dépenses (n et n-1), secteur professionnel et année
# 7 : Variable explicatives : croissance des dépenses (n et n-1) par trimestre et année
# 8 : Variable explicatives : croissance des dépenses (n et n-1) par trimestre, secteur professionnel et année

choix = 8

#Choix de l'IPC pour la correction :

# 1 : IPC : Ensemble des produits
# 2 : IPC : Enseignement
# 3 : IPC : Enseignement supérieur

IPC = 3

### Calcul de la fréquence des formations professionelles en groupant par les variables explicatives 

if (choix in([2,4,6])):
    GROUP = ['REGIO','NAFG010N']
    GROUP2 = ['LFS_REGION','NAFG010N']
elif (choix in([8])):
    GROUP = ['REGIO','NAFG010N','trim']
    GROUP2 = ['LFS_REGION','NAFG010N','trim']
elif (choix in([1,3,5])):
    GROUP = ['REGIO']
    GROUP2 = ['LFS_REGION']
elif (choix in([7])):
    GROUP = ['REGIO','trim']
    GROUP2 = ['LFS_REGION','trim']
df13num = df13[df13['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df13num = df13num[df13num['count']>2].set_index(GROUP)['count']
df13den = df13[df13['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio13 = (df13num/df13den).fillna(0).reset_index(name='ratio13').rename(columns={'REGIO':'LFS_REGION'})
ratio13 = ratio13[ratio13['ratio13']!=0]
df14num = df14[df14['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df14num = df14num[df14num['count']>2].set_index(GROUP)['count']
df14den = df14[df14['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio14 = (df14num/df14den).fillna(0).reset_index(name='ratio14').rename(columns={'REGIO':'LFS_REGION'})
ratio14 = ratio14[ratio14['ratio14']!=0]
df15num = df15[df15['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df15num = df15num[df15num['count']>2].set_index(GROUP)['count']
df15den = df15[df15['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio15 = (df15num/df15den).fillna(0).reset_index(name='ratio15').rename(columns={'REGIO':'LFS_REGION'})
ratio15 = ratio15[ratio15['ratio15']!=0]
df16num = df16[df16['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df16num = df16num[df16num['count']>2].set_index(GROUP)['count']
df16den = df16[df16['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio16 = (df16num/df16den).fillna(0).reset_index(name='ratio16').rename(columns={'REGIO':'LFS_REGION'})
ratio16 = ratio16[ratio16['ratio16']!=0]
df17num = df17[df17['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df17num = df17num[df17num['count']>2].set_index(GROUP)['count']
df17den = df17[df17['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio17 = (df17num/df17den).fillna(0).reset_index(name='ratio17').rename(columns={'REGIO':'LFS_REGION'})
ratio17 = ratio17[ratio17['ratio17']!=0]
df18num = df18[df18['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df18num = df18num[df18num['count']>2].set_index(GROUP)['count']
df18den = df18[df18['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio18 = (df18num/df18den).fillna(0).reset_index(name='ratio18').rename(columns={'REGIO':'LFS_REGION'})
ratio18 = ratio18[ratio18['ratio18']!=0]
df19num = df19[df19['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df19num = df19num[df19num['count']>2].set_index(GROUP)['count']
df19den = df19[df19['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio19 = (df19num/df19den).fillna(0).reset_index(name='ratio19').rename(columns={'REGIO':'LFS_REGION'})
ratio19 = ratio19[ratio19['ratio19']!=0]
df20num = df20[df20['FC5D']=="1"].groupby(GROUP).size().reset_index(name='count')
df20num = df20num[df20num['count']>2].set_index(GROUP)['count']
df20den = df20[df20['FC5D'].isin(("1","2"))].groupby(GROUP).size()
ratio20 = (df20num/df20den).fillna(0).reset_index(name='ratio20').rename(columns={'REGIO':'LFS_REGION'})
ratio20 = ratio20[ratio20['ratio20']!=0]

df21num = df21[df21[fnf]=="1"].groupby(GROUP2).size().reset_index(name='count')
df21num = df21num[df21num['count']>2].set_index(GROUP2)['count']
df21den = df21[df21[fnf].isin(("1","2"))].groupby(GROUP2).size()
ratio21 = (df21num/df21den).fillna(0).reset_index(name='ratio21')
ratio21 = ratio21[ratio21['ratio21']!=0]
df22num = df22[df22[fnf]=="1"].groupby(GROUP2).size().reset_index(name='count')
df22num = df22num[df22num['count']>2].set_index(GROUP2)['count']
df22den = df22[df22[fnf].isin(("1","2"))].groupby(GROUP2).size()
ratio22 = (df22num/df22den).fillna(0).reset_index(name='ratio22')
ratio22 = ratio22[ratio22['ratio22']!=0]
df23num = df23[df23[fnf]=="1"].groupby(GROUP2).size().reset_index(name='count')
df23num = df23num[df23num['count']>2].set_index(GROUP2)['count']
df23den = df23[df23[fnf].isin(("1","2"))].groupby(GROUP2).size()
ratio23 = (df23num/df23den).fillna(0).reset_index(name='ratio23')
ratio23 = ratio23[ratio23['ratio23']!=0]

### Calcul de l'évolution de cette fréquence et vérification visuelle de cohérence

if (choix in([2,4,6])):
    ON = ["LFS_REGION","NAFG010N"]
elif(choix in([8])):
    ON = ["LFS_REGION","NAFG010N",'trim']
elif(choix in([1,3,5])):
    ON = ["LFS_REGION"]
elif(choix in([7])):
    ON = ["LFS_REGION",'trim']
ratios = [ratio13,ratio14,ratio15,ratio16,ratio17,ratio18,ratio19,ratio20,ratio21,ratio22,ratio23]
ratiotot = reduce(lambda left, right : pd.merge(left,right,on=ON,how="outer"),ratios)
ratiotot["r14"]=ratiotot["ratio14"]/ratiotot["ratio13"]-1
ratiotot["r15"]=ratiotot["ratio15"]/ratiotot["ratio14"]-1
ratiotot["r16"]=ratiotot["ratio16"]/ratiotot["ratio15"]-1
ratiotot["r17"]=ratiotot["ratio17"]/ratiotot["ratio16"]-1
ratiotot["r18"]=ratiotot["ratio18"]/ratiotot["ratio17"]-1
ratiotot["r19"]=ratiotot["ratio19"]/ratiotot["ratio18"]-1
ratiotot["r22"]=ratiotot["ratio22"]/ratiotot["ratio21"]-1
ratiotot["r23"]=ratiotot["ratio23"]/ratiotot["ratio22"]-1
ratiotottemp = ratiotot[~ratiotot['LFS_REGION'].isin([101,102,103,104])]
r21m=ratiotot['ratio21'].mean()
r19m=ratiotot['ratio19'].mean()
ratiotot['ratio13c'] = ratiotot['ratio13']*r21m/r19m
ratiotot['ratio14c'] = ratiotot['ratio14']*r21m/r19m
ratiotot['ratio15c'] = ratiotot['ratio15']*r21m/r19m
ratiotot['ratio16c'] = ratiotot['ratio16']*r21m/r19m
ratiotot['ratio17c'] = ratiotot['ratio17']*r21m/r19m
ratiotot['ratio18c'] = ratiotot['ratio18']*r21m/r19m
ratiotot['ratio19c'] = ratiotot['ratio19']*r21m/r19m
ratioplot = ratiotot[['ratio13c','ratio14c','ratio15c','ratio16c','ratio17c','ratio18c','ratio19c','ratio21','ratio22','ratio23']]
plt.figure(figsize=(10,6))
for i in range(len(ratioplot)):
    plt.plot(ratioplot.columns,ratioplot.iloc[i],alpha=0.6)

plt.show()

ratiotot['LFS_REGION']=ratiotot['LFS_REGION'].astype(int)

### Création des matrices de transitions entre quintiles 

if(choix==2):
    ratioheat = ratioplot[['ratio19c','ratio21']]
    ratioheat = ratioheat.dropna()
    ratioheat['q_ratio19c'] = pd.qcut(ratioheat['ratio19c'], 5, labels = False)
    ratioheat['q_ratio21'] = pd.qcut(ratioheat['ratio21'], 5, labels = False)

    trmat = pd.crosstab(ratioheat['q_ratio19c'],ratioheat['q_ratio21'])

    plt.figure(figsize=(8,6))
    sns.heatmap(trmat, annot=True, fmt='d',cmap='Blues')
    plt.xlabel("Quintile of ratio in 2021")
    plt.ylabel("Quintile of ratio in 2019")
    plt.title("Heatmap of transition between quintiles of ratios between 2019 and 2021")
    plt.savefig("mat.png")
    plt.show()

if(choix==2):
    ratioheat2 = ratioplot[['ratio22','ratio23']]
    ratioheat2 = ratioheat2.dropna()
    ratioheat2['q_ratio22'] = pd.qcut(ratioheat2['ratio22'], 5, labels = False)
    ratioheat2['q_ratio23'] = pd.qcut(ratioheat2['ratio23'], 5, labels = False)

    trmat2 = pd.crosstab(ratioheat2['q_ratio22'],ratioheat2['q_ratio23'])

    plt.figure(figsize=(8,6))
    sns.heatmap(trmat2, annot=True, fmt='d',cmap='Blues')
    plt.xlabel("Quintile of ratio in 2023")
    plt.ylabel("Quintile of ratio in 2022")
    plt.title("Heatmap of transition between quintiles of ratios between 2022 and 2023")
    plt.savefig("mat2.png")
    plt.show()

### Importation des données comptables des régions et sélection du compte 651 de la nomenclature M71 : Formation professionelle

bal11 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2011.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal11 = bal11[bal11['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal11['SD11'] = bal11['SD'].str.replace(',','.',regex=False).astype(float)
bal11 = bal11[['CREGI','SD11']].groupby(['CREGI']).sum().reset_index()
bal11['CREGI'] = bal11['CREGI'].apply(lambda x : str(x)[:2]).map(regionsavap).astype(int)
bal12 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2012.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal12 = bal12[bal12['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal12['SD12'] = bal12['SD'].str.replace(',','.',regex=False).astype(float)
bal12 = bal12[['CREGI','SD12']].groupby(['CREGI']).sum().reset_index()
bal12['CREGI'] = bal12['CREGI'].apply(lambda x : str(x)[:2]).map(regionsavap).astype(int)
bal13 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2013.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal13 = bal13[bal13['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal13['SD13'] = bal13['SD'].str.replace(',','.',regex=False).astype(float)
bal13 = bal13[['CREGI','SD13']].groupby(['CREGI']).sum().reset_index()
bal13['CREGI'] = bal13['CREGI'].apply(lambda x : str(x)[:2]).map(regionsavap).astype(int)
bal14 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2014.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal14 = bal14[bal14['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal14['SD14'] = bal14['SD'].str.replace(',','.',regex=False).astype(float)
bal14 = bal14[['CREGI','SD14']].groupby(['CREGI']).sum().reset_index()
bal14['CREGI'] = bal14['CREGI'].apply(lambda x : str(x)[:2]).map(regionsavap).astype(int)
bal15 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2015.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal15 = bal15[bal15['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal15['SD15'] = bal15['SD'].str.replace(',','.',regex=False).astype(float)
bal15 = bal15[['CREGI','SD15']].groupby(['CREGI']).sum().reset_index()
bal15['CREGI'] = bal15['CREGI'].apply(lambda x : str(x)[:2]).map(regionsavap).astype(int)
bal16 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2016.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal16 = bal16[bal16['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal16['SD16'] = bal16['SD'].str.replace(',','.',regex=False).astype(float)
bal16 = bal16[['CREGI','SD16']].groupby(['CREGI']).sum().reset_index()
bal17 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2017.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal17 = bal17[bal17['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal17['SD17'] = bal17['SD'].str.replace(',','.',regex=False).astype(float)
bal17 = bal17[['CREGI','SD17']].groupby(['CREGI']).sum().reset_index()
bal18 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2018.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal18 = bal18[bal18['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal18['SD18'] = bal18['SD'].str.replace(',','.',regex=False).astype(float)
bal18 = bal18[['CREGI','SD18']].groupby(['CREGI']).sum().reset_index()
bal19 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2019.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal19 = bal19[bal19['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal19['SD19'] = bal19['SD'].str.replace(',','.',regex=False).astype(float)
bal19 = bal19[['CREGI','SD19']].groupby(['CREGI']).sum().reset_index()
bal20 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2020.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal20 = bal20[bal20['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal20['SD20'] = bal20['SD'].str.replace(',','.',regex=False).astype(float)
bal20 = bal20[['CREGI','SD20']].groupby(['CREGI']).sum().reset_index()
bal21 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2021.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal21 = bal21[bal21['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal21['SD21'] = bal21['SD'].str.replace(',','.',regex=False).astype(float)
bal21 = bal21[['CREGI','SD21']].groupby(['CREGI']).sum().reset_index()
bal22 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2022.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal22 = bal22[bal22['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal22['SD22'] = bal22['SD'].str.replace(',','.',regex=False).astype(float)
bal22 = bal22[['CREGI','SD22']].groupby(['CREGI']).sum().reset_index()
bal23 = pd.read_csv('C:/Users/ENSAE05_A_PERRIN0/Desktop/balancesreg/Balance_REG_2023.csv',sep=';',usecols=['COMPTE','CREGI','SD'])
bal23 = bal23[bal23['COMPTE'].apply(lambda x : str(x)[:3])=='651']
bal23['SD23'] = bal23['SD'].str.replace(',','.',regex=False).astype(float)
bal23 = bal23[['CREGI','SD23']].groupby(['CREGI']).sum().reset_index()

### Calcul de l'évolution de cette dépense 

bals = [bal11,bal12,bal13,bal14,bal15,bal16,bal17,bal18,bal19,bal20,bal21,bal22,bal23]
baltot = reduce(lambda left, right : pd.merge(left,right,on="CREGI",how="outer"),bals)
baltot["c12"]=baltot["SD12"]/baltot["SD11"]-1
baltot["c13"]=baltot["SD13"]/baltot["SD12"]-1
baltot["c14"]=baltot["SD14"]/baltot["SD13"]-1
baltot["c15"]=baltot["SD15"]/baltot["SD14"]-1
baltot["c16"]=baltot["SD16"]/baltot["SD15"]-1
baltot["c17"]=baltot["SD17"]/baltot["SD16"]-1
baltot["c18"]=baltot["SD18"]/baltot["SD17"]-1
baltot["c19"]=baltot["SD19"]/baltot["SD18"]-1
baltot["c20"]=baltot["SD20"]/baltot["SD19"]-1
baltot["c21"]=baltot["SD21"]/baltot["SD20"]-1
baltot["c22"]=baltot["SD22"]/baltot["SD21"]-1
baltot["c23"]=baltot["SD23"]/baltot["SD22"]-1
baltot['CREGI']=baltot['CREGI'].astype(int)

### Agrégation des données et suppression des lignes dont les changements de périmètres ne peuvent être retracés :

datatot = pd.merge(ratiotot,baltot,left_on='LFS_REGION',right_on='CREGI',how="outer")
datatot = datatot[~datatot['LFS_REGION'].isin([94,102,103])]

### Choix des variables de la régression 

if(choix==1):
    reg14 = datatot[['c14','r14']].rename(columns={'c14':'c','r14':'r'})
    reg14['an']=14
    reg15 = datatot[['c15','r15']].rename(columns={'c15':'c','r15':'r'})
    reg15['an']=15
    reg16 = datatot[['c16','r16']].rename(columns={'c16':'c','r16':'r'})
    reg16['an']=16
    reg17 = datatot[['c17','r17']].rename(columns={'c17':'c','r17':'r'})
    reg17['an']=17
    reg18 = datatot[['c18','r18']].rename(columns={'c18':'c','r18':'r'})
    reg18['an']=18
    reg19 = datatot[['c19','r19']].rename(columns={'c19':'c','r19':'r'})
    reg19['an']=19
    reg22 = datatot[['c22','r22']].rename(columns={'c22':'c','r22':'r'})
    reg22['an']=22
    reg23 = datatot[['c23','r23']].rename(columns={'c23':'c','r23':'r'})
    reg23['an']=23
    datareg = pd.concat([reg14,reg15,reg16,reg17,reg18,reg19,reg22,reg23],ignore_index=True)
if(choix==2):
    reg14 = datatot[['c14','r14','NAFG010N']].rename(columns={'c14':'c','r14':'r'})
    reg14['an']=14
    reg15 = datatot[['c15','r15','NAFG010N']].rename(columns={'c15':'c','r15':'r'})
    reg15['an']=15
    reg16 = datatot[['c16','r16','NAFG010N']].rename(columns={'c16':'c','r16':'r'})
    reg16['an']=16
    reg17 = datatot[['c17','r17','NAFG010N']].rename(columns={'c17':'c','r17':'r'})
    reg17['an']=17
    reg18 = datatot[['c18','r18','NAFG010N']].rename(columns={'c18':'c','r18':'r'})
    reg18['an']=18
    reg19 = datatot[['c19','r19','NAFG010N']].rename(columns={'c19':'c','r19':'r'})
    reg19['an']=19
    reg22 = datatot[['c22','r22','NAFG010N']].rename(columns={'c22':'c','r22':'r'})
    reg22['an']=22
    reg23 = datatot[['c23','r23','NAFG010N']].rename(columns={'c23':'c','r23':'r'})
    reg23['an']=23
    datareg = pd.concat([reg14,reg15,reg16,reg17,reg18,reg19,reg22,reg23],ignore_index=True)
if(choix in([3,4])):
    if(IPC==1):
        ipc = [96.91,98.78,99.70,100.18,100.44,100.63,101.32,103.37,104.58,104.79,106.34,112.55,117.65,120.20]
    if(IPC==2):
        ipc = [93.00,94.83,96.26,98.10,99.70,100.95,102.21,103.02,104.65,106.89,109.22,111.15,115.01,120.03]
    if(IPC==3):
        ipc = [94.89,96.05,97.15,99.03,99.75,100.93,101.38,101.88,103.46,105.81,108.32,110.28,114.31,119.80]
    deltaipc = [ipc[i+1]/ipc[i] for i in range(len(ipc)-1)]
    datatot['c14c']=datatot['c14']-deltaipc[2]+1
    datatot['c15c']=datatot['c15']-deltaipc[3]+1
    datatot['c16c']=datatot['c16']-deltaipc[4]+1
    datatot['c17c']=datatot['c17']-deltaipc[5]+1
    datatot['c18c']=datatot['c18']-deltaipc[6]+1
    datatot['c19c']=datatot['c19']-deltaipc[7]+1
    datatot['c20c']=datatot['c20']-deltaipc[8]+1
    datatot['c21c']=datatot['c21']-deltaipc[9]+1
    datatot['c22c']=datatot['c22']-deltaipc[10]+1
    datatot['c23c']=datatot['c23']-deltaipc[11]+1
if(choix==3):
    reg14c = datatot[['c14c','r14']].rename(columns={'c14c':'c','r14':'r'})
    reg14c['an']=14
    reg15c = datatot[['c15c','r15']].rename(columns={'c15c':'c','r15':'r'})
    reg15c['an']=15
    reg16c = datatot[['c16c','r16']].rename(columns={'c16c':'c','r16':'r'})
    reg16c['an']=16
    reg17c = datatot[['c17c','r17']].rename(columns={'c17c':'c','r17':'r'})
    reg17c['an']=17
    reg18c = datatot[['c18c','r18']].rename(columns={'c18c':'c','r18':'r'})
    reg18c['an']=18
    reg19c = datatot[['c19c','r19']].rename(columns={'c19c':'c','r19':'r'})
    reg19c['an']=19
    reg22c = datatot[['c22c','r22']].rename(columns={'c22c':'c','r22':'r'})
    reg22c['an']=22
    reg23c = datatot[['c23c','r23']].rename(columns={'c23c':'c','r23':'r'})
    reg23c['an']=23
    datareg = pd.concat([reg14c,reg15c,reg16c,reg17c,reg18c,reg19c,reg22c,reg23c],ignore_index=True)
if(choix==4):
    reg14c = datatot[['c14c','r14','NAFG010N']].rename(columns={'c14c':'c','r14':'r'})
    reg14c['an']=14
    reg15c = datatot[['c15c','r15','NAFG010N']].rename(columns={'c15c':'c','r15':'r'})
    reg15c['an']=15
    reg16c = datatot[['c16c','r16','NAFG010N']].rename(columns={'c16c':'c','r16':'r'})
    reg16c['an']=16
    reg17c = datatot[['c17c','r17','NAFG010N']].rename(columns={'c17c':'c','r17':'r'})
    reg17c['an']=17
    reg18c = datatot[['c18c','r18','NAFG010N']].rename(columns={'c18c':'c','r18':'r'})
    reg18c['an']=18
    reg19c = datatot[['c19c','r19','NAFG010N']].rename(columns={'c19c':'c','r19':'r'})
    reg19c['an']=19
    reg22c = datatot[['c22c','r22','NAFG010N']].rename(columns={'c22c':'c','r22':'r'})
    reg22c['an']=22
    reg23c = datatot[['c23c','r23','NAFG010N']].rename(columns={'c23c':'c','r23':'r'})
    reg23c['an']=23
    datareg = pd.concat([reg14c,reg15c,reg16c,reg17c,reg18c,reg19c,reg22c,reg23c],ignore_index=True)
if(choix==5):
    reg15d = datatot[['c14','c15','r15']].rename(columns={'c14':'cd','c15':'c','r15':'r'})
    reg15d['an']=15
    reg16d = datatot[['c15','c16','r16']].rename(columns={'c15':'cd','c15':'c','r16':'r'})
    reg16d['an']=16
    reg17d = datatot[['c16','c17','r17']].rename(columns={'c16':'cd','c17':'c','r17':'r'})
    reg17d['an']=17
    reg18d = datatot[['c17','c18','r18']].rename(columns={'c17':'cd','c18':'c','r18':'r'})
    reg18d['an']=18
    reg19d = datatot[['c18','c19','r19']].rename(columns={'c18':'cd','c19':'c','r19':'r'})
    reg19d['an']=19
    reg23d = datatot[['c22','c23','r23']].rename(columns={'c22':'cd','c23':'c','r23':'r'})
    reg23d['an']=23
    datareg = pd.concat([reg15d,reg16d,reg17d,reg18d,reg19d,reg23d],ignore_index=True)
if(choix==6):
    reg15d = datatot[['c14','c15','r15','NAFG010N']].rename(columns={'c14':'cd','c15':'c','r15':'r'})
    reg15d['an']=15
    reg16d = datatot[['c15','c16','r16','NAFG010N']].rename(columns={'c15':'cd','c15':'c','r16':'r'})
    reg16d['an']=16
    reg17d = datatot[['c16','c17','r17','NAFG010N']].rename(columns={'c16':'cd','c17':'c','r17':'r'})
    reg17d['an']=17
    reg18d = datatot[['c17','c18','r18','NAFG010N']].rename(columns={'c17':'cd','c18':'c','r18':'r'})
    reg18d['an']=18
    reg19d = datatot[['c18','c19','r19','NAFG010N']].rename(columns={'c18':'cd','c19':'c','r19':'r'})
    reg19d['an']=19
    reg23d = datatot[['c22','c23','r23','NAFG010N']].rename(columns={'c22':'cd','c23':'c','r23':'r'})
    reg23d['an']=23
    datareg = pd.concat([reg15d,reg16d,reg17d,reg18d,reg19d,reg23d],ignore_index=True)
if(choix==7):
    reg15d = datatot[['c14','c15','r15','trim']].rename(columns={'c14':'cd','c15':'c','r15':'r'})
    reg15d['an']=15
    reg16d = datatot[['c15','c16','r16','trim']].rename(columns={'c15':'cd','c15':'c','r16':'r'})
    reg16d['an']=16
    reg17d = datatot[['c16','c17','r17','trim']].rename(columns={'c16':'cd','c17':'c','r17':'r'})
    reg17d['an']=17
    reg18d = datatot[['c17','c18','r18','trim']].rename(columns={'c17':'cd','c18':'c','r18':'r'})
    reg18d['an']=18
    reg19d = datatot[['c18','c19','r19','trim']].rename(columns={'c18':'cd','c19':'c','r19':'r'})
    reg19d['an']=19
    reg23d = datatot[['c22','c23','r23','trim']].rename(columns={'c22':'cd','c23':'c','r23':'r'})
    reg23d['an']=23
    datareg = pd.concat([reg15d,reg16d,reg17d,reg18d,reg19d,reg23d],ignore_index=True)
    datareg['c1'] = datareg['c'].where(datareg['trim']==1,0)
    datareg['c2'] = datareg['c'].where(datareg['trim']==2,0)
    datareg['c3'] = datareg['c'].where(datareg['trim']==3,0)
    datareg['c4'] = datareg['c'].where(datareg['trim']==4,0)
    datareg['cd1'] = datareg['cd'].where(datareg['trim']==1,0)
    datareg['cd2'] = datareg['cd'].where(datareg['trim']==2,0)
    datareg['cd3'] = datareg['cd'].where(datareg['trim']==3,0)
    datareg['cd4'] = datareg['cd'].where(datareg['trim']==4,0)
if(choix==8):
    reg15d = datatot[['c14','c15','r15','NAFG010N','trim']].rename(columns={'c14':'cd','c15':'c','r15':'r'})
    reg15d['an']=15
    reg16d = datatot[['c15','c16','r16','NAFG010N','trim']].rename(columns={'c15':'cd','c15':'c','r16':'r'})
    reg16d['an']=16
    reg17d = datatot[['c16','c17','r17','NAFG010N','trim']].rename(columns={'c16':'cd','c17':'c','r17':'r'})
    reg17d['an']=17
    reg18d = datatot[['c17','c18','r18','NAFG010N','trim']].rename(columns={'c17':'cd','c18':'c','r18':'r'})
    reg18d['an']=18
    reg19d = datatot[['c18','c19','r19','NAFG010N','trim']].rename(columns={'c18':'cd','c19':'c','r19':'r'})
    reg19d['an']=19
    reg23d = datatot[['c22','c23','r23','NAFG010N','trim']].rename(columns={'c22':'cd','c23':'c','r23':'r'})
    reg23d['an']=23
    datareg = pd.concat([reg15d,reg16d,reg17d,reg18d,reg19d,reg23d],ignore_index=True)
    datareg['c1'] = datareg['c'].where(datareg['trim']==1,0)
    datareg['c2'] = datareg['c'].where(datareg['trim']==2,0)
    datareg['c3'] = datareg['c'].where(datareg['trim']==3,0)
    datareg['c4'] = datareg['c'].where(datareg['trim']==4,0)
    datareg['cd1'] = datareg['cd'].where(datareg['trim']==1,0)
    datareg['cd2'] = datareg['cd'].where(datareg['trim']==2,0)
    datareg['cd3'] = datareg['cd'].where(datareg['trim']==3,0)
    datareg['cd4'] = datareg['cd'].where(datareg['trim']==4,0)

### Visualisation

if(choix==1):
    datareg['an']=datareg['an'].apply(lambda x : str(2000+int(x)))
    sns.scatterplot(data=datareg,x='c',y='r',hue='an',palette='tab10')
    plt.title('Growth on the number of training as a function of growth of the expenditures')
    plt.xlabel('Growth of the regional expenditures')
    plt.ylabel('Growth of training')
    plt.legend(title = 'year')
    plt.savefig("graph.png")
    plt.show()

### Estimation des régressions

if(choix in([1,3])): 
    model = smf.ols("r~c+C(an)",data=datareg).fit()
if(choix in([2,4])): 
    model = smf.ols("r~c+C(an)+C(NAFG010N)",data=datareg).fit()
if(choix in([5])):
    model = smf.ols("r~c+cd+C(an)",data=datareg).fit()
if(choix in([6])):
    model = smf.ols("r~c+cd+C(an)+C(NAFG010N)",data=datareg).fit()
if(choix in([7])):
    model = smf.ols("r~c1+c2+c3+c4+cd1+cd2+cd3+cd4+C(an)",data=datareg).fit()
if(choix in([8])):
    model = smf.ols("r~c1+c2+c3+c4+cd1+cd2+cd3+cd4+C(an)+C(NAFG010N)",data=datareg).fit()
print(model.summary())
