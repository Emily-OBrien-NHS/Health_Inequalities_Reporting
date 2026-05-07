import os
import time
import shutil
import warnings
import numpy as np
import pandas as pd
import textwrap as tw
import datetime as dt
import matplotlib.pyplot as plt
from scipy.stats import mannwhitneyu
from sqlalchemy import create_engine
import scipy.stats.distributions as dist
from dateutil.relativedelta import relativedelta
os.chdir(r'G:\FBM\Operational Research\Health Inequalities')

# =============================================================================
# Initial Variables
# ============================================================================= 
#Get the current month and year for outputs
current_day = dt.datetime.today()
version_date = f'{current_day.strftime("%b")} {current_day.year}'
#Get date of last day of previous month for sql queries
end_date = (current_day.replace(day=1)
            - dt.timedelta(days=1))
start_date = end_date - relativedelta(years=1)
op_end_date = f"{end_date.strftime('%d-%B-%Y').upper()} 23:59:59"

def data_cleaning(df):
    df['IMD'] = df['IMD'].astype(float)
    #Imd and ethnicity grouping columns
    df['IMD'] = np.where(df['IMD'] <= 2, 'IMD 1-2', 'IMD 3-10')
    df['Ethnicity'] = np.where(df['Ethnicity'].isin(['Unknown', 'Unwilling to answer',
                                                     'White British']),
                               'White British', 'Ethnic Minority')
    return df

# =============================================================================
# =============================================================================
# Establish SQL Engine and Read in data
# ============================================================================= 
# =============================================================================

t0 = time.time()     
sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                              'trusted_connection=yes&driver=ODBC+Driver+17'\
                              '+for+SQL+Server')
print('Reading in data...')
SLs = pd.read_sql_query('SELECT DISTINCT(sl_desc) FROM  InfoDB.dbo.vw_cset_specialties',
                        sdmart_engine).dropna()['sl_desc'].tolist()

# ===============================
# RTT Clock Stops
rtt_low_query = f"""--RTT Clock stops
-- SELECT data from a year ago to now
DECLARE                       @StartDate AS DATETIME
DECLARE                       @Enddate AS DATETIME
SET                           @Enddate = '{op_end_date}'
SET                           @StartDate = CAST(DATEADD(YEAR, -1, @Enddate) AS Date)

--NonAdmitted Clock Stops

SELECT [IMD] = pc.IMD_Decile
,[Ethnicity] = Eth.description
,[Sex] = sex.description
,[SLC] = CASE WHEN specialty = 'Upper GI Surgery' THEN 'General, HpB, Oesophago-Gastric, Colorectal and Urology'
 ELSE spec.slc_desc END
,[SL] = spec.sl_desc
,[Specialty] = spec.pfmgt_spec_desc
,CASE WHEN alert.patnt_refno IS NOT NULL THEN 'LD' ELSE 'non-LD' END AS 'LD Flag'
,[Compl Date] = rtt_nadm.compl_dttm
,[Weeks Wait] = rtt_nadm.weeks_wait
,[Days Wait] = rtt_nadm.days_wait
FROM infodb.dbo.rtt_daily_non_admitted_snapshot rtt_nadm
LEFT JOIN PiMSMarts.dbo.patients pat --join to get latest postcode
ON ISNULL(REPLACE(rtt_nadm.nhs_number, ' ', ''), rtt_nadm.pasid) = ISNULL(pat.nhs_number, pat.pasid)
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
ON REPLACE(pat.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth ON Eth.identifier = pat.ethgr--join for ethnicity description
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON pat.sexxx = sex.identifier--join for sex description
LEFT JOIN (SELECT DISTINCT pfmgt_spec, pfmgt_spec_desc, slc_desc, sl_desc FROM infoDB.dbo.vw_cset_specialties) spec
ON rtt_nadm.specialty = spec.pfmgt_spec--join to get specialties
--Get LD patients from patient alerts
LEFT JOIN (SELECT DISTINCT patnt_refno
		   FROM PiMSMarts.dbo.Patient_Alert_Mart 
		   WHERE ODPCD_CODE = 'COM06' AND END_DTTM IS NULL) alert
ON pat.patnt_refno = alert.PATNT_REFNO
WHERE rtt_nadm.compl_dttm BETWEEN @StartDate AND @EndDate
AND (rtt_nadm.provider IN ('RK900','89006','89999','NT200','NTY00') 
     OR (rtt_nadm.provider ='XXXXX' AND rtt_nadm.clinic_code ='AC-S'))
AND cs_sorce_code <> 'DEATH' --Taken out those who were removed because they died
AND (cs_identifier <> '3' AND cs_sorce_code <> 'WLREM') -- if WLREM and 3, this also means patient died


UNION

--Admitted Clock Stops
SELECT [IMD] = pc.IMD_Decile
,[Ethnicity] = Eth.description
,[Sex] = sex.description
,[SLC] = CASE WHEN specialty = 'Upper GI Surgery' THEN 'General, HpB, Oesophago-Gastric, Colorectal and Urology'
 ELSE spec.slc_desc END
,[SL] = spec.sl_desc
,[Specialty] = specialty
,CASE WHEN alert.patnt_refno IS NOT NULL THEN 'LD' ELSE 'non-LD' END AS 'LD Flag'
,[Compl Date] = rtt_adm.admit_dttm
,[Weeks Wait] = rtt_adm.weeks_wait_unadj
,[Days Wait] = rtt_adm.days_wait_unadj

FROM InfoDB.dbo.vw_rtt_admitted_clock_stops  rtt_adm
LEFT JOIN PiMSMarts.dbo.patients pat --join to get latest postcode
ON ISNULL(REPLACE(rtt_adm.nhs_number, ' ', ''), rtt_adm.pasid) = ISNULL(pat.nhs_number,pat.pasid)
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
ON REPLACE(pat.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth ON Eth.identifier = pat.ethgr--join for ethnicity description
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON pat.sexxx = sex.identifier--join for sex description
LEFT JOIN (SELECT DISTINCT pfmgt_spec_desc, slc_desc, sl_desc
		   FROM infoDB.dbo.vw_cset_specialties) spec
ON rtt_adm.specialty = spec.pfmgt_spec_desc--join to get specialties
--Get LD patients from patient alerts
LEFT JOIN (SELECT DISTINCT patnt_refno
		   FROM PiMSMarts.dbo.Patient_Alert_Mart 
		   WHERE ODPCD_CODE = 'COM06' AND END_DTTM IS NULL) alert
ON pat.patnt_refno = alert.PATNT_REFNO
WHERE rtt_adm.type NOT IN ('Diag','NotTreated') --Removed Diag and Not Treated, so just Clock Stops
AND rtt_adm.admit_dttm BETWEEN @StartDate AND @EndDate--> '01/01/2022' 
AND rtt_adm.disch_outcome <> 'Died'
"""
rtt_low = data_cleaning(pd.read_sql_query(rtt_low_query, sdmart_engine))

# ===============================
# RTT 52 week waiters
rtt_incomp_query = """SELECT [IMD] = pc.IMD_Decile
,[Ethnicity] = eth.description
,[Sex] = sex.description
,[SLC] = spec.slc_desc
,[SL] = spec.sl_desc
,[Specialty] = Specialty_Referred_to
,[Current LoW] = RTT.current_LOW

FROM [InfoDB].[dbo].[rtt_daily_incomplete_pathways_snapshot] RTT
LEFT JOIN PiMSMarts.dbo.referrals Ref ON RTT.refrl_refno = ref.refrl_refno 
LEFT JOIN PiMSMarts.dbo.patients pat ON Ref.patnt_refno = pat.patnt_refno
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth ON Eth.identifier = pat.ethgr
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON pat.sexxx = sex.identifier--join for sex description
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
ON REPLACE(pat.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
LEFT JOIN (SELECT DISTINCT pfmgt_spec, pfmgt_spec_desc, slc_desc, sl_desc
		   FROM infoDB.dbo.vw_cset_specialties) spec
		   ON RTT.pfmgt_spec = spec.pfmgt_spec
WHERE run_date = (SELECT MAX(run_date)
FROM [InfoDB].[dbo].[rtt_daily_incomplete_pathways_snapshot])
"""
rtt_incomp = data_cleaning(pd.read_sql_query(rtt_incomp_query, sdmart_engine))

# ===============================
# Inpatient DNA Rate
IP_dna_query = """SET NOCOUNT ON
--- Spells Patient Level
SELECT [Age] = paybr.start_age
,[IMD] = pc.IMD_Decile
,[Ethnicity] = eth.description
,[Sex] = sex.description
,[SLC] = spec.slc_desc
,[SL] = spec.sl_desc
,[Category] = 'Attended'
FROM paybr_spells_2526_paybr paybr
LEFT JOIN pimsmarts.dbo.patients patnt ON paybr.patnt_refno = patnt.patnt_refno--join to get latest postcode
LEFT JOIN vw_cset_specialties spec ON paybr.local_spec = spec.local_spec--join to get specialties
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc
ON REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
LEFT JOIN pimsmarts.dbo.cset_ethgr eth ON patnt.ethgr = eth.identifier--join for ethnicity description
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON patnt.sexxx = sex.identifier--join for sex description
WHERE paybr.matpath='N' 
AND paybr.admet_nhs IN ('11','12','13')--Elective only
AND paybr.disch_dttm > dateadd(month,datediff(month,0,getdate())-12,0)
AND paybr.local_spec  <> '26' -- no elective spells expected for this, Obstetrics	

UNION ALL
--1b - IP DNAs Patient Level
SELECT		
[Age]=inpat.pat_age_on_admit
,[IMD]=pc.IMD_Decile
,[Ethnicity] = eth.description
,[Sex] = sex.description
,[SLC] = spec.slc_desc
,[SL] = spec.sl_desc
,[Category] = 'DNA'
FROM PiMSMarts.dbo.tci_histories tciii -- Main table
LEFT JOIN pimsmarts.dbo.patients patnt ON tciii.patnt_refno = patnt.patnt_refno--join to get latest postcode
LEFT JOIN vw_cset_specialties spec ON tciii.local_spec = spec.local_spec--join to get specialties
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc--join to get latest IDB info
ON REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest postcode
JOIN PiMSMarts.dbo.inpatients inpat ON tciii.wlist_refno = inpat.wlist_refno--join to get age
LEFT JOIN pimsmarts.dbo.cset_ethgr eth ON inpat.ethgr = eth.identifier--join for ethnicity description
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON patnt.sexxx = sex.identifier--join for sex description
WHERE tciii.tci_dttm > DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE())-12,0)
AND tciii.ofocm = 'DNA' -- DNA outcome
AND	 tciii.wlist_refno IN (SELECT wlist_refno 
						FROM PiMSMarts.dbo.waiting_lists_ipdc_additions
						WHERE list_name NOT LIKE '%PCH%'
						  AND list_name NOT LIKE '%PCT%' 
						  AND list_name NOT LIKE '%NR5%')
AND tciii.local_spec <> '26' -- no elective spells expected for this, Obstetrics
"""
IP_dna = data_cleaning(pd.read_sql_query(IP_dna_query, sdmart_engine))

# ===============================
# Outpatient DNA Rate
OP_dna_query = """SELECT
[Age] = vwop.pat_age_at_appt
,[IMD] = pc.IMD_Decile
,[Ethnicity] = eth.description
,[Sex] = sex.description
,[SLC] = spec.slc_desc
,[SL] = spec.sl_desc
,[Appt. Type] = vwop.visit_desc
,[Category] = vwop.attnd_desc
FROM infodb.dbo.vw_outpatients vwop
LEFT JOIN pimsmarts.dbo.patients patnt ON vwop.patnt_refno = patnt.patnt_refno--join to get latest postcode
LEFT JOIN infodb.dbo.vw_cset_specialties spec ON vwop.local_spec = spec.local_spec
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
ON REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
LEFT JOIN pimsmarts.dbo.cset_ethgr eth ON patnt.ethgr = eth.identifier--join for ethnicity description
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON patnt.sexxx = sex.identifier--join for sex description
WHERE vwop.start_dttm > DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE())-12,0)
AND cancr_dttm IS NULL--exclude cancelled appts
AND attnd IN ('3','5')--dnas and attends only for patient level
AND	vwop.sctyp = 'otpat' --added GR (only few difference)
"""
OP_dna = data_cleaning(pd.read_sql_query(OP_dna_query, sdmart_engine))

# ===============================
# F2F Appointments
F2F_query = f"""DECLARE                       @dtmStartDate AS DATETIME
DECLARE                       @dtmEnddate AS DATETIME
-- select data from a year ago to now
SET                           @dtmEnddate = '{op_end_date}'
SET                           @dtmStartDate = CAST(DATEADD(YEAR, -1, @dtmEnddate) AS Date)

SELECT [Age] = DATEDIFF(YEAR, pats.pat_dob,opact.start_dttm)
,[Age Range] = (CASE WHEN DATEDIFF(YEAR, pats.pat_dob,opact.start_dttm) < 20 THEN '0-19'
				     WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 20 AND 29 THEN '20-29'
					 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 30 AND 39 THEN '30-39'
					 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 40 AND 49 THEN '40-49'
					 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 50 AND 59 THEN '50-59'
					 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 60 AND 69 THEN '60-69'
					 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 70 AND 79 THEN '70-79'
					 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm)  >= 80 THEN '80+' END)
,[IMD] = pc.IMD_Decile
,[Ethnicity] = Eth.description
,[Sex] = sex.description
,[SLC] = spec.slc_desc
,[SL] = spec.sl_desc
,Specialty = spec.pfmgt_spec_desc
,[Category] = (CASE WHEN opact.visit IN ('1','2') THEN 'F2F'
                WHEN opact.visit IN ('3','4') THEN 'Non-F2F' END)
,[Disabled] = pats.disabled_yn 
FROM Pimsmarts.dbo.outpatients opact
LEFT JOIN InfoDB.dbo.vw_cset_specialties spec ON opact.local_spec = spec.local_spec 
LEFT JOIN PiMSMarts.dbo.patients pats ON opact.patnt_refno = pats.patnt_refno
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth  ON Eth.identifier = pats.ethgr
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON pats.sexxx = sex.identifier--join for sex description
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
ON REPLACE(pats.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
WHERE opact.cancr_dttm IS NULL 
AND opact.start_dttm BETWEEN @dtmStartDate AND @dtmEnddate
--these are standard exclusions
AND (opact.location_code NOT LIKE '5F1%' AND opact.location_code NOT LIKE '%PCT%')
AND (opact.provider IN ('RK900','89006','89999','NT200','NTY00') OR (opact.provider ='XXXXX'
AND opact.clinic_code = 'AC-S'))
AND (opact.pat_surname NOT LIKE 'ZZ%' AND opact.pat_surname NOT LIKE 'XX%')
AND opact.sctyp='OTPAT' AND opact.session_code IS NOT NULL
AND spec.pfmgt_spec <> 'ZZ' -- remove non UHP activity"""
F2F = data_cleaning(pd.read_sql_query(F2F_query, sdmart_engine))

# ===============================
# Babies < 37 week gestation
baby_37w_query = """SELECT [Mnth] = CAST(dbo.fn_get_month_start(baby.[Delivery Date / Time]) AS DATE)
,[IMD] = pc.IMD_Decile
,[Ethnicity] = eth.description
,[Sex] = sex.description
,[Gestation <37 weeks] = CASE WHEN baby.[Gestation at Delivery (weeks)] < 37 THEN 'Yes' ELSE 'No' END
FROM [InfoDB].[dbo].[RL_Maternity_upload_All] baby
LEFT JOIN pimsmarts.dbo.patients patnt ON baby.[Hospital Number Baby] = patnt.pasid
LEFT JOIN [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
ON REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
LEFT JOIN pimsmarts.dbo.cset_ethgr eth ON patnt.ethgr = eth.identifier--join for ethnicity description
LEFT JOIN pimsmarts.dbo.cset_sexxx sex ON patnt.sexxx = sex.identifier
WHERE  [Hospital Delivery Site]  = 'Derriford Hospital'
AND [Delivery Date / Time] > DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE())-12,0)"""
baby_37w = data_cleaning(pd.read_sql_query(baby_37w_query, sdmart_engine))

# ===============================
# Dispose connection
print('Data read successfully')
sdmart_engine.dispose()
# =============================================================================

# =============================================================================
# =============================================================================
# Functions
# =============================================================================
# =============================================================================
def df_to_cat_rate(df, cat_str):
    #Function to calculate the rat of category variables
    total = len(df)
    cat = len(df.loc[df['Category'] == cat_str])
    try:
        rate = cat / total
    except:
        rate = 0
    return total, rate

def propHypothesisTest(p1, p2, n1, n2):
    #Test whether two proportions are different
    #Following:https://medium.com/analytics-vidhya/testing-a-difference-in-population-proportions-in-python-89d57a06254
    #p1 and p2 are the proportions of each dataset falling in the 'yes' category
    #n1 and n2 are the total number of datapoints in each dataset
    #Alpha is the signifficance threshold (10% for this 2-tailed test)
    #Null Hypothesis: Proportions equal
    #Alternative: Proportions significantly different
    #First, find the standard error
    #For this, we need the total proportion with a yes classification
    try:
        p = (n1*p1 + n2*p2)/(n1 + n2)
        se = np.sqrt(p*(1-p)*((1/n1) + (1/n2)))
        #Next, calculate the test statistic:
            #(best estimate - hypothesized estimate)/standard error
            #best estimate = p1-p2, hypothesized = 0(as p1 and p2 are equal)
        if se == 0:
            test_stat, pvalue = np.nan, np.nan
            return test_stat, pvalue
        test_stat = (p1-p2)/se 
        #This gives number of standard deviations from hypothesized estimate
        #From the test statistic, get the p-value
        pvalue = 2 * dist.norm.cdf(-np.abs(test_stat)) # Multiplied by two indicates a two tailed testing.
    except:
        test_stat, pvalue = np.nan, np.nan
    return test_stat, pvalue

def cont_vals(df, col):
    #Function to get values, totals and pvalues of a coninuous dataset (e.g LoW) for plotting
    #ETHNICITY
    me = df.loc[df['Ethnicity']=='Ethnic Minority'].copy()
    wb = df.loc[df['Ethnicity']=='White British'].copy()
    me_value, wb_value = me[col].median(), wb[col].median()
    me_total, wb_total = me[col].count(), wb[col].count()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        eth_pval = mannwhitneyu(me[col].dropna().tolist(), wb[col].dropna().tolist())[1]

    #IMD
    imd_12 = df.loc[df['IMD'] == 'IMD 1-2'].copy()
    imd_310 = df.loc[df['IMD'] == 'IMD 3-10'].copy()
    imd_1_2_value, imd_3_10_value = imd_12[col].median(), imd_310[col].median()
    imd_1_2_total, imd_3_10_total = imd_12[col].count(), imd_310[col].count()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        imd_pval = mannwhitneyu(imd_12[col].dropna().tolist(), imd_310[col].dropna().tolist())[1]

    #SEX
    fe = df.loc[df['Sex'] == 'Female'].copy()
    ma = df.loc[df['Sex'] == 'Male'].copy()
    fe_value, ma_value = fe[col].median(), ma[col].median()
    fe_total, ma_total = fe[col].count(), ma[col].count()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sex_pval = mannwhitneyu(fe[col].dropna().tolist(), ma[col].dropna().tolist())[1]

    #OUTPUT LISTS
    values = [me_value, wb_value, imd_1_2_value, imd_3_10_value, fe_value, ma_value]
    totals = [me_total, wb_total, imd_1_2_total, imd_3_10_total, fe_total, ma_total]
    pvals = [eth_pval, imd_pval, sex_pval]
    return values, totals, pvals

def prop_vals(df, cat_str):
    #Function to get values, totals and pvalues of a proportion dataset (e.g DNA rate) for plotting
    #ETHNICITY
    me_total, me_rate = df_to_cat_rate(df.loc[df['Ethnicity'] == 'Ethnic Minority'].copy(), cat_str)
    wb_total, wb_rate = df_to_cat_rate(df.loc[df['Ethnicity'] == 'White British'].copy(), cat_str)
    eth_z_value, eth_pvalue = propHypothesisTest(me_rate, wb_rate, me_total, wb_total)
    #IMD
    imd_1_2_total, imd_1_2_rate = df_to_cat_rate(df.loc[df['IMD'] == 'IMD 1-2'].copy(), cat_str)
    imd_3_10_total, imd_3_10_rate = df_to_cat_rate(df.loc[df['IMD'] == 'IMD 3-10'].copy(), cat_str)
    imd_z_value, imd_pvalue = propHypothesisTest(imd_1_2_rate, imd_3_10_rate, imd_1_2_total, imd_3_10_total)
    #SEX
    fe_total, fe_rate = df_to_cat_rate(df.loc[df['Sex'] == 'Female'].copy(), cat_str)
    ma_total, ma_rate = df_to_cat_rate(df.loc[df['Sex'] == 'Male'].copy(), cat_str)
    sex_z_value, sex_pvalue = propHypothesisTest(fe_rate, ma_rate, fe_total, ma_total)
    #OUTPUT LISTS
    values = [me_rate, wb_rate, imd_1_2_rate, imd_3_10_rate, fe_rate, ma_rate]
    totals = [me_total, wb_total, imd_1_2_total, imd_3_10_total, fe_total, ma_total]
    pvals = [eth_pvalue, imd_pvalue, sex_pvalue]
    return values, totals, pvals

def show_values_on_bars(axs, percentage = True, multiply=True, numbers = None):
    #Function to plot labels on bars
    def _show_on_single_plot(ax):
        counter = 0       
        for p in ax.patches:
            if p._height !=0:
                if percentage:
                    _x = p.get_x() + p.get_width() / 2
                    #if (p.xy[1] == 0) and (p._height < 1):#p._height < 0.50:
                    _y = p.get_y() + p.get_height() + 0.01
                    if multiply:
                        value = '{:.2f}'.format(p.get_height()*100)
                    else:
                        value = '{:.2f}'.format(p.get_height())
                    if numbers:
                        ax.text(_x, _y+0.02, value+"%\n("+str(numbers[counter])+")",
                                ha="center")
                        counter = counter + 1
                    else:
                        ax.text(_x, _y, value+"%", ha="center")
                else:
                    _x = p.get_x() + p.get_width() / 2
                    _y = p.get_y() + p.get_height()+0.3
                    value = '{:.1f}'.format(p.get_height())
                    ax.text(_x, _y, value, ha="center") 
    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _show_on_single_plot(ax)
    else:
        _show_on_single_plot(axs)

def label_diff(ax, i, j, text, X, Y):
    #Significance bars
    x = (X[i] + X[j]) / 2
    y = 1.07 * max(Y[i], Y[j])
    props = {'connectionstyle':'bar', 'arrowstyle':'-', 'shrinkA':20,
             'shrinkB':20,'linewidth':2}
    ylims = ax.get_ylim()[1] - ax.get_ylim()[0]
    #If its a percentage plot, don't need extra y increase
    if max(Y) == min(Y) == 1:
        ax.annotate(text, xy=(x, y*1.05), zorder=10, ha='center',
                    annotation_clip=False)
        ax.annotate('', xy=(X[i], y*0.87), xytext=(X[j], y*0.87),
                    arrowprops=props, annotation_clip=False)
    else:
        ax.annotate(text, xy=(x, y+0.2*ylims), zorder=10, ha='center')
        ax.annotate('', xy=(X[i], y), xytext=(X[j], y), arrowprops=props)        

def plot_label(pvalue, val1, val2, cat_str, ineq_cat):
    #Function to create the siginifcance labels for plots and tables.
    if pvalue < 0.05:
        kind = 'higher' if val1 > val2 else 'lower'
        label = tw.fill(f'Significantly {kind} {cat_str} for {ineq_cat} patients', 35)
    elif pvalue == pvalue:
        kind = np.nan
        label = tw.fill(f'No significant difference in {cat_str} for {ineq_cat} patients', 35)
    else:
        kind = 'NA'
        label = 'Unable to perform analysis'
    return label, kind

def create_plot(title, cat_str, sl, values, totals, pvals, perc):
    #multiply values by 100 to get percentage
    if perc:
        values = [i*100 for i in values]
    #Plot bar chart
    fig, ax = plt.subplots(1,1, figsize=(11,6))
    ax.bar([1,2,3,4,5,6], values,
           color = ['royalblue','lightskyblue','seagreen','lightgreen', 'purple', 'orchid'],
           edgecolor='black')
    ax.set_xticks([1,2,3,4,5,6])
    ax.set_xticklabels([f'Ethnic\n Minority\n({totals[0]:,.0f}\nappointments)',
                        f'White British\n({totals[1]:,.0f}\nappointments)',
                        f'IMD 1-2\n({totals[2]:,.0f}\nappointments)',
                        f'IMD 3-10\n({totals[3]:,.0f}\nappointments)',
                        f'Female\n({totals[4]:,.0f}\nappointments)',
                        f'Male\n({totals[5]:,.0f}\nappointments)'])
    ax.set_ylabel(cat_str)
    ax.set_title(title)
    show_values_on_bars(ax, percentage=perc, multiply=False)

    #Add labels for significant difference if they occur.
    eth_label, eth_kind = plot_label(pvals[0], values[0], values[1], cat_str, 'Ethnic Minority')
    label_diff(ax, 0, 1, eth_label, [1,2,3,4,5,6], values)
    imd_label, imd_kind = plot_label(pvals[1], values[2], values[3], cat_str, 'IMD 1&2')
    label_diff(ax, 2, 3, imd_label, [1,2,3,4,5,6], values)
    sex_label, sex_kind = plot_label(pvals[2], values[4], values[5], cat_str, 'Female')
    label_diff(ax, 4, 5, sex_label, [1,2,3,4,5,6], values)

    plt.ylim(ymax=ax.get_ylim()[1]*1.4)

    #Save figure, put in SL folder if SL plot
    if sl == sl:
        plt.savefig(f'Plots/{sl}/{title} - {sl}.png', bbox_inches='tight')
    else:
        #Save fig in overall data
        plt.savefig(f'Plots/_ALL/{title}.png', bbox_inches='tight')
    plt.close()

    #Output the values for the summary tables
    out_lst = ([title, sl, eth_kind, imd_kind, sex_kind] + values + totals)
    return out_lst
# =============================================================================

# =============================================================================
# =============================================================================
# Create Plots
# =============================================================================
# =============================================================================
###########Median LoW
rtt_low_lst = []
low_values, low_totals, low_pvalues = cont_vals(rtt_low, 'Days Wait')
rtt_low_lst.append(create_plot('RTT Current LoW', 'days wait', np.nan, low_values, low_totals, low_pvalues, False))

###########52 week waiters
rtt_incomp['Category'] = np.where(rtt_incomp['Current LoW'] > 364, '52+ Wks', '<52 Wks')
rtt_52ww_lst = []
rtt_52ww_values, rtt_52ww_totals, rtt_52ww_pvals = prop_vals(rtt_incomp, '52+ Wks')
rtt_52ww_lst.append(create_plot('RTT 52+ Week Waiters', 'proportion of >52 week waits', np.nan, rtt_52ww_values, rtt_52ww_totals, rtt_52ww_pvals, True))

###########Inpatient DNAs
IP_dnas_lst = []
IP_dna_values, IP_dna_totals, IP_dna_pvals = prop_vals(IP_dna, 'DNA')
IP_dnas_lst.append(create_plot('Inpatients DNA Rate', 'DNA rate',  np.nan, IP_dna_values, IP_dna_totals, IP_dna_pvals, True))

###########Outpatient DNAs
OP_dnas_lst = []
OP_dna_values, OP_dna_totals, OP_dna_pvals = prop_vals(OP_dna, 'DNA')
OP_dnas_lst.append(create_plot('Outpatients DNA Rate', 'DNA rate',  np.nan, OP_dna_values, OP_dna_totals, OP_dna_pvals, True))

############Non face to face apts
F2F_lst = []
F2F_values, F2F_totals, F2F_pvals = prop_vals(F2F, 'Non-F2F')
F2F_lst.append(create_plot('Non-F2F Appointments', 'Non-F2F appt rate', np.nan, F2F_values, F2F_totals, F2F_pvals, True))

############Babies <37 weeks
baby_37w['Category'] = baby_37w['Gestation <37 weeks'].copy()
baby_values, baby_totals, baby_pvals = prop_vals(baby_37w, 'Yes')
baby_lst = create_plot('Babies born under 37 Weeks Gestation', 'proportion of babies', np.nan, baby_values, baby_totals, baby_pvals, True)

###########Service Line Plots
for sl in SLs:
    #create directory if doesn't exist
    os.makedirs(f'Plots/{sl}', exist_ok=True)
    #Median LoW
    low_values, low_totals, low_pvalues = cont_vals(rtt_low.loc[rtt_low['SL'] == sl].copy(), 'Days Wait')
    rtt_low_lst.append(create_plot('RTT Current LoW', 'days wait', sl, low_values, low_totals, low_pvalues, False))
    #RTT 52+ Week Waits
    rtt_52ww_values, rtt_52ww_totals, rtt_52ww_pvals = prop_vals(rtt_incomp.loc[rtt_incomp['SL'] == sl].copy(), '52+ Wks')
    rtt_52ww_lst.append(create_plot('RTT 52+ Week Waiters', 'proportion of >52 week waits', sl, rtt_52ww_values, rtt_52ww_totals, rtt_52ww_pvals, True))
    #IP DNAs
    IP_dna_values, IP_dna_totals, IP_dna_pvals = prop_vals(IP_dna.loc[IP_dna['SL'] == sl].copy(), 'DNA')
    IP_dnas_lst.append(create_plot('Inpatients DNA Rate', 'DNA rate',  sl, IP_dna_values, IP_dna_totals, IP_dna_pvals, True))
    #OP DNAs
    OP_dna_values, OP_dna_totals, OP_dna_pvals = prop_vals(OP_dna.loc[OP_dna['SL'] == sl].copy(), 'DNA')
    OP_dnas_lst.append(create_plot('Outpatients DNA Rate', 'DNA rate',  sl, OP_dna_values, OP_dna_totals, OP_dna_pvals, True))
    #F2F Appts
    F2F_values, F2F_totals, F2F_pvals = prop_vals(F2F.loc[F2F['SL'] == sl].copy(), 'Non-F2F')
    F2F_lst.append(create_plot('Non-F2F Appointments', 'Non-F2F appt rate', sl, F2F_values, F2F_totals, F2F_pvals, True))

#copy and paste the gestation plot into the maternity folder.
shutil.copy(r'G:\FBM\Operational Research\Health Inequalities\Plots\_ALL\Babies born under 37 Weeks Gestation.png',
            r'G:\FBM\Operational Research\Health Inequalities\Plots\Maternity Services')
# =============================================================================

# =============================================================================
# =============================================================================
# Create summary tables
# =============================================================================
# =============================================================================
#List of columns for the summary tables
df_cols = ['Data Set', 'Service Line', 'Ethnicity', 'IMD', 'Sex', 'Minority Ethnic Rate',
           'White British Rate', 'IMD 1&2 Rate', 'IMD 3-10 Rate', 'Female Rate', 'Male Rate',
           'Minority Ethnic Total', 'White British Total', 'IMD 1&2 Total', 'IMD 3-10 Total',
           'Female Total', 'Male Total']

#Final Dfs
RTT_LoW = pd.DataFrame(rtt_low_lst, columns=df_cols)
RTT_52w = pd.DataFrame(rtt_52ww_lst, columns=df_cols)
IP_dnas = pd.DataFrame(IP_dnas_lst, columns=df_cols)
OP_dnas = pd.DataFrame(OP_dnas_lst, columns=df_cols)
F2F_apt = pd.DataFrame(F2F_lst, columns=df_cols)
baby_37 = pd.DataFrame([baby_lst], columns=df_cols)

#Create Excel output
with pd.ExcelWriter(f'Tables/HI Summary Table - {version_date}.xlsx', engine='xlsxwriter') as writer:
    RTT_LoW.to_excel(writer, sheet_name='Median LoW', index=False)
    RTT_52w.to_excel(writer, sheet_name='52+ Week Wait', index=False)
    IP_dnas.to_excel(writer, sheet_name='Inpatient DNAs', index=False)
    OP_dnas.to_excel(writer, sheet_name='Outpatient DNAs', index=False)
    F2F_apt.to_excel(writer, sheet_name='Non F2F Appts', index=False)
    baby_37.to_excel(writer, sheet_name='Babies <37 Weeks', index=False)


