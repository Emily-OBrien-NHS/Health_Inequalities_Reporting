import pandas as pd
from sqlalchemy import create_engine
import textwrap as tw
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy.stats import mannwhitneyu
from matplotlib.ticker import PercentFormatter
import matplotlib.dates as mdates
import scipy.stats.distributions as dist
import datetime as dt
import itertools
import win32com.client as win32
from dateutil.relativedelta import relativedelta
from pptx import Presentation
from pptx.util import Inches, Pt
import time
import os
os.chdir(r'G:\PerfInfo\Performance Management\OR Team\Emily Projects\Inequalities\Health Inequalities Reporting')

#Get the current month and year for outputs
current_day = dt.datetime.today()
version_date = f'{current_day.strftime("%b")} {current_day.year}'
#Get date of last day of previous month for sql queries
end_date = (current_day.replace(day=1)
            - dt.timedelta(days=1))
start_date = end_date - relativedelta(years=1)
op_end_date = f"{end_date.strftime('%d-%B-%Y').upper()} 23:59:59"

# =============================================================================
# % Get data from queries
# ============================================================================= 
t0 = time.time()     
sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                              'trusted_connection=yes&driver=ODBC+Driver+17'\
                              '+for+SQL+Server')
print('Reading in data...')
print('Clock Stops query running...')
# RTT Clock stops - Data Retrieval and formatting
query = f"""
DECLARE                       @StartDate AS DATETIME
DECLARE                       @Enddate AS DATETIME
-- SELECT data from a year ago to now
SET                           @Enddate = '{op_end_date}'
SET                           @StartDate = CAST(DATEADD(YEAR, -1, @Enddate) AS Date)
--Get RTT clock stops since 01/01/2022, disregarding those removed because they died
--NonAdmitted Clock Stops
SELECT rtt_nadm.compl_dttm, rtt_nadm.weeks_wait, rtt_nadm.days_wait,
ISNULL(REPLACE(rtt_nadm.nhs_number, ' ', ''), rtt_nadm.pasid) AS pat_no,
pat.pat_pcode, imd.[IndexValue] AS 'Decile ', Eth.[description],
spec.pfmgt_spec_desc AS specialty,
CASE WHEN specialty = 'Upper GI Surgery' THEN 'General, HpB, Oesophago-Gastric, Colorectal and Urology'
ELSE spec.slc_desc END AS SLC,
spec.div_code
--,case when alert.patnt_refno is not NULL then 'LD' else 'non-LD' end as 'LD Flag'
FROM infodb.dbo.rtt_daily_non_admitted_snapshot  rtt_nadm
--Get postcode from patients
LEFT JOIN PiMSMarts.dbo.patients pat
ON ISNULL(REPLACE(rtt_nadm.nhs_number, ' ', ''), rtt_nadm.pasid) = ISNULL(pat.nhs_number, pat.pasid)
--Get IMD from postcode
LEFT JOIN [PiMSMarts].[Reference].[vw_IndicesOfMultipleDeprivation2019_DecileByPostcode] imd
ON pat.pat_pcode = imd.PostcodeFormatted
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth
ON Eth.identifier = pat.ethgr
LEFT JOIN (SELECT DISTINCT pfmgt_spec, pfmgt_spec_desc, slc_desc, div_code FROM infoDB.dbo.vw_cset_specialties) spec
ON rtt_nadm.specialty = spec.pfmgt_spec
--Get LD patients from patient alerts
LEFT JOIN (SELECT DISTINCT patnt_refno
		   FROM PiMSMarts.dbo.Patient_Alert_Mart 
		   WHERE ODPCD_CODE = 'COM06' AND END_DTTM IS NULL) alert
ON pat.patnt_refno=alert.PATNT_REFNO
WHERE
(rtt_nadm.compl_dttm BETWEEN @StartDate AND @EndDate)--> '01/01/2022')
AND (rtt_nadm.provider IN ('RK900','89006','89999','NT200','NTY00') 
OR (rtt_nadm.provider ='XXXXX' 
AND rtt_nadm.clinic_code ='AC-S'))
AND cs_sorce_code <> 'DEATH' --Taken out those who were removed because they died
AND (cs_identifier <> '3' AND cs_sorce_code <> 'WLREM') -- if WLREM and 3, this also means patient died
AND imd.EndDate IS NULL
UNION
--Admitted Clock Stops
SELECT rtt_adm.admit_dttm AS compl_dttm, rtt_adm.weeks_wait_unadj, rtt_adm.days_wait_unadj,
ISNULL(rtt_adm.nhs_number,rtt_adm.pasid) AS pat_no, pat.pat_pcode, imd.[IndexValue] AS 'Decile ',
Eth.[description], specialty,
CASE WHEN specialty = 'Upper GI Surgery' THEN 'General, HpB, Oesophago-Gastric, Colorectal and Urology'
ELSE spec.slc_desc END AS SLC, div_code
--,case when alert.patnt_refno is not NULL then 'LD' else 'non-LD' end as 'LD Flag'
FROM InfoDB.dbo.vw_rtt_admitted_clock_stops  rtt_adm
--Get postcode from patients
LEFT JOIN PiMSMarts.dbo.patients pat
ON ISNULL(REPLACE(rtt_adm.nhs_number, ' ', ''), rtt_adm.pasid) = ISNULL(pat.nhs_number,pat.pasid)
--Get IMD from postcode
LEFT JOIN [PiMSMarts].[Reference].[vw_IndicesOfMultipleDeprivation2019_DecileByPostcode] imd
ON pat.pat_pcode = imd.PostcodeFormatted
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth
ON Eth.identifier = pat.ethgr
LEFT JOIN (SELECT DISTINCT pfmgt_spec_desc, slc_desc, div_code
		   FROM infoDB.dbo.vw_cset_specialties) spec
ON rtt_adm.specialty = spec.pfmgt_spec_desc
--Get LD patients from patient alerts
LEFT JOIN (SELECT DISTINCT patnt_refno
		   FROM PiMSMarts.dbo.Patient_Alert_Mart 
		   WHERE ODPCD_CODE = 'COM06' AND END_DTTM IS NULL) alert
ON pat.patnt_refno=alert.PATNT_REFNO
WHERE rtt_adm.type NOT IN ('Diag','NotTreated') --Removed Diag and Not Treated, so just Clock Stops
AND rtt_adm.admit_dttm BETWEEN @StartDate AND @EndDate--> '01/01/2022' 
AND rtt_adm.disch_outcome <> 'Died'
AND imd.EndDate IS NULL
"""
rtt_cs = pd.read_sql(query, sdmart_engine)
print('Clock Stops query complete')
#Get IMD deciles 1&2 and all others. Also don't include NaN days wait
rtt_cs['Decile '] = rtt_cs['Decile '].astype(float)

# RTT WL - Data Retrieval
print('RTT Wait List query running...')
wl_query = """
--Will return current RTT incomplete waiting list position
SELECT imd.[IndexValue] AS 'Decile ', RTT.current_LOW, eth.[description],
Specialty_Referred_to AS specialty, spec.slc_desc AS SLC,
div_code
FROM [InfoDB].[dbo].[rtt_daily_incomplete_pathways_snapshot] RTT
LEFT JOIN PiMSMarts.dbo.referrals Ref
ON RTT.refrl_refno = ref.refrl_refno 
LEFT JOIN PiMSMarts.dbo.patients pat
ON Ref.patnt_refno = pat.patnt_refno
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth
ON Eth.identifier = pat.ethgr
LEFT JOIN [PiMSMarts].[Reference].[vw_IndicesOfMultipleDeprivation2019_DecileByPostcode] imd
ON pat.pat_pcode = imd.PostcodeFormatted
--left join [InfoDB].[dbo].[IMDScore] imd
			--on pat.pat_pcode = imd.PCD2
LEFT JOIN (SELECT DISTINCT pfmgt_spec, pfmgt_spec_desc, slc_desc, div_code
		   FROM infoDB.dbo.vw_cset_specialties) spec
		   ON RTT.pfmgt_spec = spec.pfmgt_spec
WHERE run_date = (SELECT MAX(run_date)
FROM [InfoDB].[dbo].[rtt_daily_incomplete_pathways_snapshot])
AND imd.EndDate IS NULL
"""
rtt_incomp = pd.read_sql(wl_query, sdmart_engine)
print('RTT Wait List query complete') 
#Make Decile column numeric
rtt_incomp['Decile '] = rtt_incomp['Decile '].astype(float)

# Non-F2F Section Data retrieval
print('OP query running...')
op_query = f"""
DECLARE                       @dtmStartDate AS DATETIME
DECLARE                       @dtmEnddate AS DATETIME
-- select data from a year ago to now
SET                           @dtmEnddate = '{op_end_date}'
SET                           @dtmStartDate = CAST(DATEADD(YEAR, -1, @dtmEnddate) AS Date)

SELECT opact.pasid,
Visit = CASE WHEN opact.visit IN ('1','2') THEN 'F2F'
		WHEN opact.visit IN ('3','4') THEN 'Non-F2F' END,
DATEDIFF(YEAR, pats.pat_dob,opact.start_dttm) AS Age,
CASE WHEN DATEDIFF(YEAR, pats.pat_dob,opact.start_dttm) < 20 THEN '0-19'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 20 AND 29 THEN '20-29'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 30 AND 39 THEN '30-39'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 40 AND 49 THEN '40-49'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 50 AND 59 THEN '50-59'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 60 AND 69 THEN '60-69'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm) BETWEEN 70 AND 79 THEN '70-79'
	 WHEN DATEDIFF(YEAR,pats.pat_dob,opact.start_dttm)  >= 80 THEN '80+'
	 END AS Age_range,
specialty = spec.pfmgt_spec_desc, slc = spec.slc_desc, [Eth].[description] AS Ethnicity,
imd.[IndexValue], pats.disabled_yn                
FROM Pimsmarts.dbo.outpatients opact
LEFT JOIN InfoDB.dbo.vw_cset_specialties spec
ON opact.local_spec = spec.local_spec 
LEFT JOIN PiMSMarts.dbo.patients pats
ON opact.patnt_refno = pats.patnt_refno
LEFT JOIN PiMSMarts.dbo.cset_ethgr Eth 
ON Eth.identifier = pats.ethgr
LEFT JOIN [PiMSMarts].[Reference].[vw_IndicesOfMultipleDeprivation2019_DecileByPostcode] imd
ON opact.pat_pcode = imd.PostcodeFormatted
WHERE opact.cancr_dttm IS NULL 
AND opact.start_dttm BETWEEN @dtmStartDate AND @dtmEnddate
--these are standard exclusions
AND (opact.location_code NOT LIKE '5F1%' AND opact.location_code NOT LIKE '%PCT%')
AND (opact.provider IN ('RK900','89006','89999','NT200','NTY00') OR (opact.provider ='XXXXX'
AND opact.clinic_code = 'AC-S'))
AND (opact.pat_surname NOT LIKE 'ZZ%' AND opact.pat_surname NOT LIKE 'XX%')
AND opact.sctyp='OTPAT' AND opact.session_code IS NOT NULL
AND spec.pfmgt_spec <> 'ZZ' -- remove non UHP activity
AND imd.EndDate IS NULL
"""
#op_data = pd.read_sql(op_query, sdmart_engine)
op_data = pd.read_excel('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/OutpatientData.xlsx')
print('OP query complete')
op_data['IndexValue'] = op_data['IndexValue'].astype(float)
#print timings
t1 = time.time()
sdmart_engine.dispose()
print(f'Queries run in {(t1-t0)/60} mins')


#Test whether two proportions are different
def propHypothesisTest(p1, p2, n1, n2, alpha = 0.05):
    #Following:https://medium.com/analytics-vidhya/testing-a-difference-in-population-proportions-in-python-89d57a06254
    #p1 and p2 are the proportions of each dataset falling in the 'yes' category
    #n1 and n2 are the total number of datapoints in each dataset
    #Alpha is the signifficance threshold (10% for this 2-tailed test)
    #Null Hypothesis: Proportions equal
    #Alternative: Proportions significantly different
    #First, find the standard error
    #For this, we need the total proportion with a yes classification
    p = (n1*p1 + n2*p2)/(n1 + n2)
    se = np.sqrt(p*(1-p)*((1/n1) + (1/n2)))
    #Next, calculate the test statistic:
        #(best estimate - hypothesized estimate)/standard error
        #best estimate = p1-p2, hypothesized = 0(as p1 and p2 are equal)
    if se == 0:
        return None
    test_stat = (p1-p2)/se 
    #This gives number of standard deviations from hypothesized estimate
    #From the test statistic, get the p-value
    pvalue = 2 * dist.norm.cdf(-np.abs(test_stat)) # Multiplied by two indicates a two tailed testing.
    return pvalue

#function to get list of counts to account for missing values
def counts_list(counts, options):
    c1 = (counts[options[0]] if options[0] in counts.index else 0)
    c2 = (counts[options[1]] if options[1] in counts.index else 0)
    return [c1, c2]


# =============================================================================
#     #Meidan LoW by specialty
# =============================================================================
out = []
for spec in rtt_cs['specialty'].drop_duplicates().tolist():
    rtt_slc = rtt_cs.loc[rtt_cs['specialty'] == spec].copy()
    sample_size = len(rtt_slc)
    division = rtt_slc['div_code'].iloc[0]

    #Get IMD deciles 1&2 and all others. Also don't include NaN days wait
    imd_1_2 = rtt_slc.loc[(rtt_slc['Decile '].isin([1,2]))
                        & (~pd.isnull(rtt_slc['days_wait']))].copy()
    imd_3_10 = rtt_slc.loc[(~rtt_slc['Decile '].isin([1,2]))
                        & (~pd.isnull(rtt_slc['days_wait']))
                        & (~pd.isnull(rtt_slc['Decile ']))].copy()
    #Get minority ethnicities and non
    me = rtt_slc.loc[(~rtt_slc['description'].isin(
                    ['Unknown', 'Unwilling to answer', 'White British']))
                    & (~pd.isnull(rtt_slc['days_wait']))].copy()
    wb = rtt_slc.loc[(rtt_slc['description'] == 'White British')
                    & (~pd.isnull(rtt_slc['days_wait']))].copy()
    #Find median length of wait for each of these populations
    
    me_med_low = me['days_wait'].median()
    wb_med_low = wb['days_wait'].median()
    imd_1_2_med_low = imd_1_2['days_wait'].median()
    imd_3_10_med_low = imd_3_10['days_wait'].median()
    #pvalues
    pval_rtt_cs_IMD = mannwhitneyu(imd_1_2['days_wait'].tolist(),
                               imd_3_10['days_wait'].tolist(),
                               alternative = 'greater')[1]
    pval_rtt_cs_eth = mannwhitneyu(me['days_wait'].tolist(),
                                wb['days_wait'].tolist(),
                                alternative = 'greater')[1]
    
    #Add labels for significant difference if they occur.
    out.append([division, spec, sample_size, me_med_low, wb_med_low, imd_1_2_med_low, imd_3_10_med_low, pval_rtt_cs_eth, pval_rtt_cs_IMD])

SPEC_med_LoW = pd.DataFrame(out, columns=['Division', 'Specialty', 'No. Patients', 'Minority Ethnic', 'White British', 'IMD 1-2', 'IMD 3-10', 'Ethnicity Pval', 'IMD Pval'])
SPEC_med_LoW[['Ethnicity', 'Deprivation']] = 'No Difference'
SPEC_med_LoW.loc[SPEC_med_LoW['Ethnicity Pval'] < 0.05, 'Ethnicity'] = 'Significant Difference'
SPEC_med_LoW.loc[SPEC_med_LoW['Ethnicity Pval'].isna(), 'Ethnicity'] = 'Insufficient Data'
SPEC_med_LoW.loc[SPEC_med_LoW['IMD Pval'] < 0.05, 'Deprivation'] = 'Significant Difference'
SPEC_med_LoW.loc[SPEC_med_LoW['IMD Pval'].isna(), 'Deprivation'] = 'Insufficient Data'


# =============================================================================
#     #Meidan LoW by division
# =============================================================================
out = []
for div in ['A', 'B', 'C', 'D']:
    rtt_slc = rtt_cs.loc[rtt_cs['div_code'] == div].copy()
    sample_size = len(rtt_slc)

    #Get IMD deciles 1&2 and all others. Also don't include NaN days wait
    imd_1_2 = rtt_slc.loc[(rtt_slc['Decile '].isin([1,2]))
                        & (~pd.isnull(rtt_slc['days_wait']))].copy()
    imd_3_10 = rtt_slc.loc[(~rtt_slc['Decile '].isin([1,2]))
                        & (~pd.isnull(rtt_slc['days_wait']))
                        & (~pd.isnull(rtt_slc['Decile ']))].copy()
    #Get minority ethnicities and non
    me = rtt_slc.loc[(~rtt_slc['description'].isin(
                    ['Unknown', 'Unwilling to answer', 'White British']))
                    & (~pd.isnull(rtt_slc['days_wait']))].copy()
    wb = rtt_slc.loc[(rtt_slc['description'] == 'White British')
                    & (~pd.isnull(rtt_slc['days_wait']))].copy()
    #Find median length of wait for each of these populations
    
    me_med_low = me['days_wait'].median()
    wb_med_low = wb['days_wait'].median()
    imd_1_2_med_low = imd_1_2['days_wait'].median()
    imd_3_10_med_low = imd_3_10['days_wait'].median()
    #pvalues
    pval_rtt_cs_IMD = mannwhitneyu(imd_1_2['days_wait'].tolist(),
                               imd_3_10['days_wait'].tolist(),
                               alternative = 'greater')[1]
    pval_rtt_cs_eth = mannwhitneyu(me['days_wait'].tolist(),
                                wb['days_wait'].tolist(),
                                alternative = 'greater')[1]
    
    #Add labels for significant difference if they occur.
    out.append([div, sample_size, me_med_low, wb_med_low, imd_1_2_med_low, imd_3_10_med_low, pval_rtt_cs_eth, pval_rtt_cs_IMD])

DIV_med_LoW = pd.DataFrame(out, columns=['Division', 'No. Patients', 'Minority Ethnic', 'White British', 'IMD 1-2', 'IMD 3-10', 'Ethnicity Pval', 'IMD Pval'])
DIV_med_LoW[['Ethnicity', 'Deprivation']] = 'No Difference'
DIV_med_LoW.loc[DIV_med_LoW['Ethnicity Pval'] < 0.05, 'Ethnicity'] = 'Significant Difference'
DIV_med_LoW.loc[DIV_med_LoW['Ethnicity Pval'].isna(), 'Ethnicity'] = 'Insufficient Data'
DIV_med_LoW.loc[DIV_med_LoW['IMD Pval'] < 0.05, 'Deprivation'] = 'Significant Difference'
DIV_med_LoW.loc[DIV_med_LoW['IMD Pval'].isna(), 'Deprivation'] = 'Insufficient Data'



# ========================================================================
#     #>52 week wait SPEC
# ========================================================================
out = []
for spec in rtt_incomp['specialty'].drop_duplicates().tolist():
    data = rtt_incomp.loc[rtt_incomp['specialty'] == spec].copy()
    sample_size = len(data)
    sample_size_gt52 = len(data.loc[data['current_LOW'] > 364])
    division = data['div_code'].iloc[0]

    if sample_size_gt52 > 0:
        #####IMD
        #Get a version with no NaNs for deciles
        rtt_incomp_dec = data.loc[~pd.isnull(rtt_incomp['Decile '])].copy()
        #Make new columns with IMD1-2 or IMD3-10
        rtt_incomp_dec['value'] = np.where(rtt_incomp_dec['Decile '].isin([1, 2]),
                                        'IMD 1-2', 'IMD 3-10')
        rtt_incomp_dec['type'] = 'IMD'
        #Filter to >52 weeks
        rtt_incomp_52_dec = rtt_incomp_dec.loc[rtt_incomp['current_LOW'] > 364].copy()
        rtt_incomp_52_dec['type'] = 'IMD\n (>52 Week Wait)'

        #####Ethnicity
        #Get a version without unknown ethnicities
        rtt_incomp_eth = data.loc[~rtt_incomp['description']
                                        .isin(['Unknown','Unwilling to answer'])].copy()
        #Add column for white british and ethnic minority split
        rtt_incomp_eth['value'] = np.where(rtt_incomp_eth['description']
                                        == 'White British',
                                        'White British', 'Ethnic Minority')
        rtt_incomp_eth['type'] = 'Ethnicity'
        #Filter to >52 weeks
        rtt_incomp_52_eth = rtt_incomp_eth.loc[rtt_incomp['current_LOW'] > 364].copy()
        rtt_incomp_52_eth['type'] = 'Ethnicity \n(>52 Week Wait)'

        ##############Hypothesis testing
        #####IMD
        #Get counts
        total_num_IMD = rtt_incomp_dec.shape[0]
        total_num_IMD_52 = rtt_incomp_52_dec.shape[0]
        n_IMD12 = rtt_incomp_dec[rtt_incomp_dec['value'] == 'IMD 1-2'].shape[0]
        n_IMD12_52 = rtt_incomp_52_dec[rtt_incomp_52_dec['value'] == 'IMD 1-2'].shape[0]
        try:
            #test hypothesis
            pval_rtt_incomp_imd = propHypothesisTest((n_IMD12 / total_num_IMD),
                                                    (n_IMD12_52 / total_num_IMD_52),
                                                    n_IMD12, n_IMD12_52, alpha=0.05)
        except:
            pval_rtt_incomp_imd = np.nan

        #####Ethnicity
        #get counts
        total_num_eth = rtt_incomp_eth.shape[0]
        total_num_eth_52 = rtt_incomp_52_eth.shape[0]
        n_em = rtt_incomp_eth[rtt_incomp_eth['value'] == 'Ethnic Minority'].shape[0]
        n_em_52 = rtt_incomp_52_eth[rtt_incomp_52_eth['value']
                                    == 'Ethnic Minority'].shape[0]
        #test hypothesis
        try:
            pval_rtt_incomp_eth = propHypothesisTest((n_em / total_num_eth),
                                                    (n_em_52 / total_num_eth_52),
                                                    n_em, n_em_52, alpha=0.05)
        except:
            pval_rtt_incomp_eth = np.nan

            #Add labels for significant difference if they occur.
        out.append([division, spec, sample_size, n_em, n_IMD12, sample_size_gt52,  n_em_52, n_IMD12_52, pval_rtt_incomp_eth, pval_rtt_incomp_imd])

SPEC_gt52 = pd.DataFrame(out, columns=['Division', 'Specialty', 'No. Patients', 'No. Minority Ethnic', 'No. IMD 1-2', 'No. Patients >52wk', 'No. Minority Ethnic >52wk', 'No. IMD 1-2 >52wk', 'Ethnicity Pval', 'IMD Pval'])
SPEC_gt52[['Ethnicity', 'Deprivation']] = 'No Difference'
SPEC_gt52.loc[SPEC_gt52['Ethnicity Pval'] < 0.05, 'Ethnicity'] = 'Significant Difference'
SPEC_gt52.loc[SPEC_gt52['Ethnicity Pval'].isna(), 'Ethnicity'] = 'Insufficient Data'
SPEC_gt52.loc[SPEC_gt52['IMD Pval'] < 0.05, 'Deprivation'] = 'Significant Difference'
SPEC_gt52.loc[SPEC_gt52['IMD Pval'].isna(), 'Deprivation'] = 'Insufficient Data'



# ========================================================================
#     #>52 week wait DIV
# ========================================================================
out = []
for div in ['A', 'B', 'C', 'D']:
    data = rtt_incomp.loc[rtt_incomp['div_code'] == div].copy()
    sample_size = len(data)
    sample_size_gt52 = len(data.loc[data['current_LOW'] > 364])

    if sample_size_gt52 > 0:
        #####IMD
        #Get a version with no NaNs for deciles
        rtt_incomp_dec = data.loc[~pd.isnull(rtt_incomp['Decile '])].copy()
        #Make new columns with IMD1-2 or IMD3-10
        rtt_incomp_dec['value'] = np.where(rtt_incomp_dec['Decile '].isin([1, 2]),
                                        'IMD 1-2', 'IMD 3-10')
        rtt_incomp_dec['type'] = 'IMD'
        #Filter to >52 weeks
        rtt_incomp_52_dec = rtt_incomp_dec.loc[rtt_incomp['current_LOW'] > 364].copy()
        rtt_incomp_52_dec['type'] = 'IMD\n (>52 Week Wait)'

        #####Ethnicity
        #Get a version without unknown ethnicities
        rtt_incomp_eth = data.loc[~rtt_incomp['description']
                                        .isin(['Unknown','Unwilling to answer'])].copy()
        #Add column for white british and ethnic minority split
        rtt_incomp_eth['value'] = np.where(rtt_incomp_eth['description']
                                        == 'White British',
                                        'White British', 'Ethnic Minority')
        rtt_incomp_eth['type'] = 'Ethnicity'
        #Filter to >52 weeks
        rtt_incomp_52_eth = rtt_incomp_eth.loc[rtt_incomp['current_LOW'] > 364].copy()
        rtt_incomp_52_eth['type'] = 'Ethnicity \n(>52 Week Wait)'

        ##############Hypothesis testing
        #####IMD
        #Get counts
        total_num_IMD = rtt_incomp_dec.shape[0]
        total_num_IMD_52 = rtt_incomp_52_dec.shape[0]
        n_IMD12 = rtt_incomp_dec[rtt_incomp_dec['value'] == 'IMD 1-2'].shape[0]
        n_IMD12_52 = rtt_incomp_52_dec[rtt_incomp_52_dec['value'] == 'IMD 1-2'].shape[0]
        try:
            #test hypothesis
            pval_rtt_incomp_imd = propHypothesisTest((n_IMD12 / total_num_IMD),
                                                    (n_IMD12_52 / total_num_IMD_52),
                                                    n_IMD12, n_IMD12_52, alpha=0.05)
        except:
            pval_rtt_incomp_imd = np.nan
        imd_str = 'a' if pval_rtt_incomp_imd < 0.05 else 'no'

        #####Ethnicity
        #get counts
        total_num_eth = rtt_incomp_eth.shape[0]
        total_num_eth_52 = rtt_incomp_52_eth.shape[0]
        n_em = rtt_incomp_eth[rtt_incomp_eth['value'] == 'Ethnic Minority'].shape[0]
        n_em_52 = rtt_incomp_52_eth[rtt_incomp_52_eth['value']
                                    == 'Ethnic Minority'].shape[0]
        #test hypothesis
        try:
            pval_rtt_incomp_eth = propHypothesisTest((n_em / total_num_eth),
                                                    (n_em_52 / total_num_eth_52),
                                                    n_em, n_em_52, alpha=0.05)
        except:
            pval_rtt_incomp_eth = np.nan

        eth_str = 'a' if pval_rtt_incomp_eth < 0.05 else 'no'

            #Add labels for significant difference if they occur.
        out.append([div, sample_size, n_em, n_IMD12, sample_size_gt52,  n_em_52, n_IMD12_52, pval_rtt_incomp_eth, pval_rtt_incomp_imd])

DIVgt52 = pd.DataFrame(out, columns=['Division', 'No. Patients', 'No. Minority Ethnic', 'No. IMD 1-2', 'No. Patients >52wk', 'No. Minority Ethnic >52wk', 'No. IMD 1-2 >52wk', 'Ethnicity Pval', 'IMD Pval'])
DIVgt52[['Ethnicity', 'Deprivation']] = 'No Difference'
DIVgt52.loc[DIVgt52['Ethnicity Pval'] < 0.05, 'Ethnicity'] = 'Significant Difference'
DIVgt52.loc[DIVgt52['Ethnicity Pval'].isna(), 'Ethnicity'] = 'Insufficient Data'
DIVgt52.loc[DIVgt52['IMD Pval'] < 0.05, 'Deprivation'] = 'Significant Difference'
DIVgt52.loc[DIVgt52['IMD Pval'].isna(), 'Deprivation'] = 'Insufficient Data'


# ======================================================================
#         #F2F SPEC
# ======================================================================

out = []
for spec in op_data['specialty'].drop_duplicates().tolist():
    data = op_data.loc[op_data['specialty'] == spec].copy()
    sample_size = len(data)
    nonf2f_size = len(data.loc[data['Visit'] == 'Non-F2F'])
    division = data['Division'].iloc[0]

    ####IMD
    op_data_temp_imd = data.loc[~pd.isnull(op_data['IndexValue'])].copy()
    op_data_temp_imd['value'] = np.where(op_data_temp_imd['IndexValue']
                                                .isin([1, 2]), 'IMD 1-2', 'IMD 3-10')
    #Make a list of the numbers to include under the percentages
    nonf2f_counts_imd = op_data_temp_imd.loc[op_data_temp_imd['Visit'] == 'Non-F2F',
                                        'value'].value_counts()
    counts_imd = op_data_temp_imd['value'].value_counts()
    try:
        imd = counts_imd['IMD 1-2']
    except:
        imd = 0
    try:    
        nonf2f_imd = nonf2f_counts_imd['IMD 1-2']
    except:
        nonf2f_imd = 0

    #Proportion testing
    try:  
        pval_op_imd = propHypothesisTest(nonf2f_counts_imd['IMD 1-2']/counts_imd['IMD 1-2'],
                                        nonf2f_counts_imd['IMD 3-10']/counts_imd['IMD 3-10'],
                                        nonf2f_counts_imd['IMD 1-2'], nonf2f_counts_imd['IMD 3-10'], alpha=0.025)
    except:
        pval_op_imd = np.nan

    ####Ethnicity
    op_data_temp_eth = data.loc[~op_data['Ethnicity']
                            .isin(['Unknown', 'Unwilling to answer'])].copy()
    op_data_temp_eth['value'] = np.where(op_data_temp_eth['Ethnicity']
                                            == 'White British',
                                            'White British', 'Ethnic Minority')
    #Make a list of the numbers to include under the percentages
    nonf2f_counts = op_data_temp_eth.loc[op_data_temp_eth['Visit'] == 'Non-F2F',
                                        'value'].value_counts()
    counts = op_data_temp_eth['value'].value_counts()

    try:
        eth = counts['Ethnic Minority']
    except:
        eth = 0
    try:    
        nonf2f_eth = nonf2f_counts['Ethnic Minority']
    except:
        nonf2f_eth = 0

    #proportion testing
    try:
        pval_op_eth = propHypothesisTest(nonf2f_counts['Ethnic Minority']/counts['Ethnic Minority'],
                                        nonf2f_counts['White British']/counts['White British'],
                                        nonf2f_counts['Ethnic Minority'], nonf2f_counts['White British'], alpha=0.025)
    except:
        pval_op_eth = np.nan
    
    out.append([division, spec, sample_size, eth, imd, nonf2f_size, nonf2f_eth, nonf2f_imd, pval_op_eth, pval_op_imd])

SPEC_f2f = pd.DataFrame(out, columns=['Division', 'Specialty', 'No. Patients', 'No. Minority Ethnic', 'No. IMD 1-2',  'Non f2f', 'Non f2f Minority Ethnic', 'Non f2f IMD 1-2', 'Ethnicity Pval', 'IMD Pval'])
SPEC_f2f[['Ethnicity', 'Deprivation']] = 'No Difference'
SPEC_f2f.loc[SPEC_f2f['Ethnicity Pval'] < 0.05, 'Ethnicity'] = 'Significant Difference'
SPEC_f2f.loc[SPEC_f2f['Ethnicity Pval'].isna(), 'Ethnicity'] = 'Insufficient Data'
SPEC_f2f.loc[SPEC_f2f['IMD Pval'] < 0.05, 'Deprivation'] = 'Significant Difference'
SPEC_f2f.loc[SPEC_f2f['IMD Pval'].isna(), 'Deprivation'] = 'Insufficient Data'


# ======================================================================
#         #F2F div
# ======================================================================

out = []
for div in ['A', 'B', 'C', 'D']:
    data = op_data.loc[op_data['Division'] == div].copy()
    sample_size = len(data)
    nonf2f_size = len(data.loc[data['Visit'] == 'Non-F2F'])

    ####IMD
    op_data_temp_imd = data.loc[~pd.isnull(op_data['IndexValue'])].copy()
    op_data_temp_imd['value'] = np.where(op_data_temp_imd['IndexValue']
                                                .isin([1, 2]), 'IMD 1-2', 'IMD 3-10')
    #Make a list of the numbers to include under the percentages
    nonf2f_counts_imd = op_data_temp_imd.loc[op_data_temp_imd['Visit'] == 'Non-F2F',
                                        'value'].value_counts()
    counts_imd = op_data_temp_imd['value'].value_counts()
    try:
        imd = counts_imd['IMD 1-2']
    except:
        imd = 0
    try:    
        nonf2f_imd = nonf2f_counts_imd['IMD 1-2']
    except:
        nonf2f_imd = 0

    #Proportion testing
    try:  
        pval_op_imd = propHypothesisTest(nonf2f_counts_imd['IMD 1-2']/counts_imd['IMD 1-2'],
                                        nonf2f_counts_imd['IMD 3-10']/counts_imd['IMD 3-10'],
                                        nonf2f_counts_imd['IMD 1-2'], nonf2f_counts_imd['IMD 3-10'], alpha=0.025)
    except:
        pval_op_imd = np.nan

    ####Ethnicity
    op_data_temp_eth = data.loc[~op_data['Ethnicity']
                            .isin(['Unknown', 'Unwilling to answer'])].copy()
    op_data_temp_eth['value'] = np.where(op_data_temp_eth['Ethnicity']
                                            == 'White British',
                                            'White British', 'Ethnic Minority')
    #Make a list of the numbers to include under the percentages
    nonf2f_counts = op_data_temp_eth.loc[op_data_temp_eth['Visit'] == 'Non-F2F',
                                        'value'].value_counts()
    counts = op_data_temp_eth['value'].value_counts()

    try:
        eth = counts['Ethnic Minority']
    except:
        eth = 0
    try:    
        nonf2f_eth = nonf2f_counts['Ethnic Minority']
    except:
        nonf2f_eth = 0

    #proportion testing
    try:
        pval_op_eth = propHypothesisTest(nonf2f_counts['Ethnic Minority']/counts['Ethnic Minority'],
                                        nonf2f_counts['White British']/counts['White British'],
                                        nonf2f_counts['Ethnic Minority'], nonf2f_counts['White British'], alpha=0.025)
    except:
        pval_op_eth = np.nan
    
    out.append([div, sample_size, eth, imd, nonf2f_size, nonf2f_eth, nonf2f_imd, pval_op_eth, pval_op_imd])

DIV_f2f = pd.DataFrame(out, columns=['Division', 'No. Patients', 'No. Minority Ethnic', 'No. IMD 1-2',  'Non f2f', 'Non f2f Minority Ethnic', 'Non f2f IMD 1-2', 'Ethnicity Pval', 'IMD Pval'])
DIV_f2f[['Ethnicity', 'Deprivation']] = 'No Difference'
DIV_f2f.loc[DIV_f2f['Ethnicity Pval'] < 0.05, 'Ethnicity'] = 'Significant Difference'
DIV_f2f.loc[DIV_f2f['Ethnicity Pval'].isna(), 'Ethnicity'] = 'Insufficient Data'
DIV_f2f.loc[DIV_f2f['IMD Pval'] < 0.05, 'Deprivation'] = 'Significant Difference'
DIV_f2f.loc[DIV_f2f['IMD Pval'].isna(), 'Deprivation'] = 'Insufficient Data'




#tO EXCEL
with pd.ExcelWriter("Health Inequalities.xlsx") as writer:
    DIV_med_LoW.to_excel(writer, sheet_name="Division Median LoW", index=False)
    SPEC_med_LoW.to_excel(writer, sheet_name="Specialty Median LoW", index=False)

    DIVgt52.to_excel(writer, sheet_name="Division 52 week", index=False)
    SPEC_gt52.to_excel(writer, sheet_name="Specialty 52 week", index=False)

    DIV_f2f.to_excel(writer, sheet_name="Division f2f", index=False)
    SPEC_f2f.to_excel(writer, sheet_name="Specialty f2f", index=False)