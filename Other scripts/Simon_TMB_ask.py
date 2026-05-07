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
import scipy.stats
import time
import os
os.chdir(r'G:\PerfInfo\Performance Management\OR Team\Emily Projects\Inequalities\Health Inequalities Reporting')

# =============================================================================
# % Get data from queries
# ============================================================================= 
t0 = time.time()     
sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                              'trusted_connection=yes&driver=ODBC+Driver+17'\
                              '+for+SQL+Server')

##Inpatients
IP_df_sql = """--use infodb
SET NOCOUNT ON
--1--Patient Level Output - below provides row detail on Spells and DNAs for IP for FY 2025/26

--1a - Spells Patient Level - 97,860 rows as at 10th April 2026
Select 
[patnt_age]=paybr.start_age
,[IMD_decile]=pc.IMD_Decile
,[ethnicity] = eth.description
,spec.slc_desc
,spec.sl_desc
,category = 'Attended'
From 
paybr_spells_2526_paybr paybr
left join pimsmarts.dbo.patients patnt on paybr.patnt_refno = patnt.patnt_refno--join to get latest postcode
left join vw_cset_specialties spec on paybr.local_spec = spec.local_spec
left join [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
on REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
left join pimsmarts.dbo.cset_ethgr eth on patnt.ethgr = eth.identifier--join for ethnicity description
Where paybr.matpath='N' 
and paybr.admet_nhs in('11','12','13')--Elective only
and paybr.disch_dttm > dateadd(month,datediff(month,0,getdate())-12,0)
--and paybr.disch_dttm between '01-Apr-2025' and '31-Mar-2026 23:59:59'
and paybr.local_spec  <> '26' -- no elective spells expected for this, Obstetrics	

union all

--1b - IP DNAs Patient Level - 540 rows as at 10th April
Select		
[patnt_age]=inpat.pat_age_on_admit
,[IMD_decile]=pc.IMD_Decile
,[ethnicity] = eth.description
,spec.slc_desc
,spec.sl_desc
,category = 'DNA'
From		
PiMSMarts.dbo.tci_histories tciii -- Main table
left join vw_cset_specialties spec on tciii.local_spec = spec.local_spec
left join pimsmarts.dbo.patients patnt on tciii.patnt_refno = patnt.patnt_refno--join to get latest postcode
join PiMSMarts.dbo.inpatients inpat on tciii.wlist_refno = inpat.wlist_refno--join to get age
left join [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc--join to get latest IDB info 
on REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest postcode
left join pimsmarts.dbo.cset_ethgr eth on inpat.ethgr = eth.identifier--join for ethnicity description
Where		
tciii.tci_dttm > dateadd(month,datediff(month,0,getdate())-12,0)
--tciii.tci_dttm between '01-Apr-2025' and '31-Mar-2026 23:59:59'
and			tciii.ofocm = 'DNA' -- DNA outcome
and			tciii.wlist_refno in(select wlist_refno 
						From PiMSMarts.dbo.waiting_lists_ipdc_additions
						where list_name not like '%PCH%'
						and list_name not like '%PCT%' 
						and list_name not like '%NR5%')
and tciii.local_spec <> '26' -- no elective spells expected for this, Obstetrics	
"""
IP_df = pd.read_sql(IP_df_sql, sdmart_engine)
IP_df['IMD_decile'] = IP_df['IMD_decile'].astype(float)
#Imd and ethnicity grouping columns
IP_df['IMD'] = np.where(IP_df['IMD_decile'] <= 2, 'IMD 1-2', 'IMD 3-10')
IP_df['eth'] = np.where(IP_df['ethnicity'].isin(['Unknown', 'Unwilling to answer', 'White British']), 'White British', 'Ethnic Minority')

OP_df_sql = """
Select 
[Patnt_refno]=vwop.patnt_refno
,[patnt_age]=vwop.pat_age_at_appt
,[IMD_decile]=pc.IMD_Decile
,spec.slc_desc
,spec.sl_desc
,[ethnicity] = eth.description
,[appt_type] = vwop.visit_desc
,[category] = vwop.attnd_desc
--,[mnth]=cast(dbo.fn_get_month_start(vwop.start_dttm)as date)
From 
infodb.dbo.vw_outpatients vwop
left join infodb.dbo.vw_cset_specialties spec on vwop.local_spec = spec.local_spec
left join pimsmarts.dbo.patients patnt on vwop.patnt_refno = patnt.patnt_refno--join to get latest postcode
left join [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
on REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
left join pimsmarts.dbo.cset_ethgr eth on patnt.ethgr = eth.identifier--join for ethnicity description
Where vwop.start_dttm > dateadd(month,datediff(month,0,getdate())-12,0)
--vwop.start_dttm between '01-Apr-2025' and '31-Mar-2026 23:59:59'--FY 202526  change to rollwing 12 months
and cancr_dttm is null--exclude cancelled appts
and attnd in ('3','5')--dnas and attends only for patient level
and	vwop.sctyp='otpat'   --added GR (only few difference)
"""
OP_df = pd.read_sql(OP_df_sql, sdmart_engine)
OP_df['IMD_decile'] = OP_df['IMD_decile'].astype(float)
#Imd and ethnicity grouping columns
OP_df['IMD'] = np.where(OP_df['IMD_decile'] <= 2, 'IMD 1-2', 'IMD 3-10')
OP_df['eth'] = np.where(OP_df['ethnicity'].isin(['Unknown', 'Unwilling to answer', 'White British']), 'White British', 'Ethnic Minority')

#Maternity
mat_df_sql = """Select  [IMD_decile]=pc.IMD_Decile
,[ethnicity] = eth.description
,[Gestation <37 weeks] = case when baby.[Gestation at Delivery (weeks)] < 37 then 'Yes' else 'No' end
,[mnth]=cast(dbo.fn_get_month_start(baby.[Delivery Date / Time])as date)
from   [InfoDB].[dbo].[RL_Maternity_upload_All] baby
left join pimsmarts.dbo.patients patnt on baby.[Hospital Number Baby] = patnt.pasid
left join [PiMSMarts].[Reference].[IndicesOfMultipleDeprivation2025] pc 
on REPLACE(patnt.[pat_pcode], ' ', '') = pc.[Postcode]--join to get latest IDB info
left join pimsmarts.dbo.cset_ethgr eth on patnt.ethgr = eth.identifier--join for ethnicity description
Where  [Hospital Delivery Site]  = 'Derriford Hospital'
and  [Delivery Date / Time] > dateadd(month,datediff(month,0,getdate())-12,0)
"""
mat_df = pd.read_sql(mat_df_sql, sdmart_engine)

# =============================================================================
# % Functions
# =============================================================================
#Function to plot labels on bars
def show_values_on_bars(axs, percentage = True, multiply=True, numbers = None):
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
        
#Function to show totals at top of bars
def show_totals(axs, totals):
    def _show_single_totals(ax):
        counter = 0
        for p in ax.patches:
            if p._height != 0:
                _x = p.get_x() + p.get_width() / 2
                if ((p.xy[1] > 0) and (p._height < 1)) or (p._height == 1):#p._height > 0.50:
                    _y = p.get_y() + p.get_height()-0.05
                    ax.text(_x, _y, str(totals[counter]), ha="center")
                    counter = counter + 1
    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _show_single_totals(ax)
    else:
        _show_single_totals(axs)

#Significance bars
def label_diff(ax, i, j, text, X, Y):
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

#Test whether two proportions are different
def propHypothesisTest(p1, p2, n1, n2):
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

def df_to_dna_rate(df):
    total = len(df)
    dnas = len(df.loc[df['category'] == 'DNA'])
    try:
        dna_rate = dnas / total
    except:
        dna_rate = 0
    return total, dna_rate

# =============================================================================
#     #Make a bar chart of the median dna for IMD and Eth categories SLIDE 2
# =============================================================================
def slide2(df, type_name, sl):
    #split into groups, get the total apts and dns rate for each
    imd_1_2_total, imd_1_2_rate = df_to_dna_rate(df.loc[df['IMD'] == 'IMD 1-2'].copy())
    imd_3_10_total, imd_3_10_rate = df_to_dna_rate(df.loc[df['IMD'] == 'IMD 3-10'].copy())

    me_total, me_rate = df_to_dna_rate(df.loc[df['eth'] == 'Ethnic Minority'].copy())
    wb_total, wb_rate = df_to_dna_rate(df.loc[df['eth'] == 'White British'].copy())

    #Test for statistical difference in wait length for IMD and Ethnicity
    imd_dna_z_value, imd_dna_pvalue = propHypothesisTest(imd_1_2_rate, imd_3_10_rate, imd_1_2_total, imd_3_10_total)
    eth_dna_z_value, eth_dna_pvalue = propHypothesisTest(me_rate, wb_rate, me_total, wb_total)

    values = [me_rate, wb_rate, imd_1_2_rate, imd_3_10_rate]
    values_100 = [i*100 for i in values]
    #Plot bar chart
    fig,ax = plt.subplots(1,1)
    ax.bar([1, 2, 3, 4], values_100,
        color = ['royalblue','lightskyblue','seagreen','lightgreen'],
        edgecolor='black')
    ax.set_xticks([1,2,3,4])
    ax.set_xticklabels(['Ethnic\n Minority\n('+f"{me_total:,.0f}"+'\n appointments',
                    'White British\n('+f"{wb_total:,.0f}"+'\n appointments)',
                    'IMD 1-2\n('+f"{imd_1_2_total:,.0f}"+'\n appointments)',
                    'IMD 3-10\n('+f"{imd_3_10_total:,.0f}"+'\n appointments)'])
    ax.set_ylabel('DNA Rate')
    ax.set_title(f'DNA Rate for {type_name} attendances')
    show_values_on_bars(ax, percentage = True, multiply=False)

    #Add labels for significant difference if they occur.
    eth_kind = np.nan
    if eth_dna_pvalue < 0.05:
        eth_kind = 'higher' if me_rate > wb_rate else 'lower'
        label = f'Significantly {eth_kind} DNA rate\nfor Ethnic Minority patients'
    elif eth_dna_pvalue == eth_dna_pvalue: #check if not nan
        label = 'No significant difference in\n DNA rate for Ethnic Minority patients'
    else:
        label = 'Unable to perform analysis'
    
    label_diff(ax, 0, 1, label, [1,2,3,4], values_100)
    
    imd_kind = np.nan
    if imd_dna_pvalue < 0.05:
        imd_kind = 'higher' if imd_1_2_rate > imd_3_10_rate else 'lower'
        label = f'Significantly {imd_kind} DNA rate\nfor IMD 1&2 patients'
    elif imd_dna_pvalue == imd_dna_pvalue:
        label = 'No significant difference\nin DNA rate for IMD 1&2 patients'
    else:
        label = 'Unable to perform analysis'
    
    label_diff(ax, 2, 3, label, [1,2,3,4], values_100)

    plt.ylim(ymax=ax.get_ylim()[1]*1.4)
    if sl:
        plt.savefig(f'plots/AdHoc/{type_name.split(' ')[-1]} Service line/AdHoc - {type_name}.png', bbox_inches='tight')
    else:
        plt.savefig(f'plots/AdHoc/AdHoc - {type_name}.png', bbox_inches='tight')
    plt.close()

    out_lst = ([type_name, eth_kind, imd_kind] + values_100
               + [me_total, wb_total, imd_1_2_total, imd_3_10_total])

    return out_lst

#Overall
ip_lst = slide2(IP_df, 'Inpatient', False)
op_lst = slide2(OP_df, 'Outpatient', False)

#Serviceline
sl_lsts = []
for sl in IP_df['sl_desc'].drop_duplicates().dropna().values.tolist():
    data = IP_df.loc[IP_df['sl_desc'] == sl].copy()
    if len(data['category'].drop_duplicates()) > 1:
        sl_lsts.append(slide2(IP_df.loc[IP_df['sl_desc'] == sl].copy(), f'{sl} Inpatient', True))
    else:
        print(f'no IP DNAs for {sl}')
        ETH = data['eth'].value_counts()
        ETH_wb = ETH['White British'] if 'White British' in ETH.index else 0
        ETH_me = ETH['Ethnic Minority'] if 'Ethnic Minority' in ETH.index else 0
        IMD = data['IMD'].value_counts()
        IMD_1_2 = IMD['IMD 1-2'] if 'IMD 1-2' in IMD.index else 0
        IMD_3_10 = IMD['IMD 3-10'] if 'IMD 3-10' in IMD.index else 0
        sl_lsts.append([sl, 'No DNAs', 'No DNAs', 0, 0, 0, 0, ETH_me, ETH_wb, IMD_1_2, IMD_3_10])
IP_sl_df = pd.DataFrame(sl_lsts, columns=['Service Line', 'Ethnicity', 'IMD 1&2',
                                        'me rate', 'wb rate', 'IMD 1&2 rate', 'IMD 3-10 rate',
                                        'me total', 'wb total', 'IMD 1&2 total', 'IMD 3-10 total'])

#TO DO: REPEAT FOR OPs
sl_lsts = []
for sl in OP_df['sl_desc'].drop_duplicates().dropna().values.tolist():
    data = OP_df.loc[OP_df['sl_desc'] == sl].copy()
    if len(data['category'].drop_duplicates()) > 1:
        sl_lsts.append(slide2(OP_df.loc[OP_df['sl_desc'] == sl].copy(), f'{sl} Outpatient', True))
    else:
        print(f'no OP DNAs for {sl}')
        ETH = data['eth'].value_counts()
        ETH_wb = ETH['White British'] if 'White British' in ETH.index else 0
        ETH_me = ETH['Ethnic Minority'] if 'Ethnic Minority' in ETH.index else 0
        IMD = data['IMD'].value_counts()
        IMD_1_2 = IMD['IMD 1-2'] if 'IMD 1-2' in IMD.index else 0
        IMD_3_10 = IMD['IMD 3-10'] if 'IMD 3-10' in IMD.index else 0
        sl_lsts.append([sl, 'No DNAs', 'No DNAs', 0, 0, 0, 0, ETH_me, ETH_wb, IMD_1_2, IMD_3_10])
OP_sl_df = pd.DataFrame(sl_lsts, columns=['Service Line', 'Ethnicity', 'IMD 1&2',
                                        'me rate', 'wb rate', 'IMD 1&2 rate', 'IMD 3-10 rate',
                                        'me total', 'wb total', 'IMD 1&2 total', 'IMD 3-10 total'])

# =============================================================================
# # ETHNICITY SLC RTT MEDIAN LOW - statistical tests SLIDE 3
# =============================================================================

def slide_3(col, col_name, df, type_name):
    slc_unique = df[col].dropna().unique()
    wb = df.loc[df['eth'] == 'White British'].copy()
    me = df.loc[df['eth'] == 'Ethnic Minority'].copy()
    #loop over each slc
    eth_rates = []
    wb_rates = []
    DNA_pvals = []
    for spec in slc_unique:
        wb_spec = wb.loc[wb[col] == spec].copy()
        me_spec = me.loc[me[col] == spec].copy()
        #Only perform Mood's median test if there are t least 15 samples in each 
        #pop. Also, if both medians are zero, median test cannot work properly, so 
        #exclude
        me_total, me_rate = df_to_dna_rate(me_spec)
        wb_total, wb_rate = df_to_dna_rate(wb_spec)
        #significance testing
        if (wb_spec.shape[0] >= 15) and (me_spec.shape[0] >= 15):
            #Test for statistical difference in wait length for IMD and Ethnicity
            eth_dna_z_value, eth_dna_pvalue = propHypothesisTest(me_rate, wb_rate, me_total, wb_total)
            DNA_pvals.append(eth_dna_pvalue)
        else:
            DNA_pvals.append(np.nan)
        #add rates
        eth_rates.append(me_rate*100)
        wb_rates.append(wb_rate*100)

    #Make a df of the results
    DNA_eth_pvals = pd.DataFrame({col_name:slc_unique, 
                                'p-value':DNA_pvals,
                                'DNA Rate WB':wb_rates,
                                'DNA Rate ME':eth_rates})
    #statistically significant results
    DNA_eth_pvals = DNA_eth_pvals.loc[DNA_eth_pvals['p-value'] < 0.025].copy()

    DNA_eth_pvals_plt = pd.melt(DNA_eth_pvals.loc[DNA_eth_pvals['p-value'] < 0.025].copy(),
                              id_vars=[col_name],
                              value_vars = ['DNA Rate ME', 'DNA Rate WB'])
    if len(DNA_eth_pvals_plt) > 0:
        #Plot
        fig, ax_spec_eth = plt.subplots(1, 1, figsize = (20,6))
        sns.barplot(data=DNA_eth_pvals_plt, x=col_name, hue = 'variable', y='value',
                    ax=ax_spec_eth, palette=['royalblue','lightskyblue'])
        legend = ax_spec_eth.get_legend()
        # #Get seaborn legend
        handles = legend.legend_handles
        ax_spec_eth.legend(handles, ['Ethnic Minority', 'White British'],)
                        #bbox_to_anchor = (1,1))
        ax_spec_eth.set_ylabel('DNA Rate %')
        ax_spec_eth.set_xticks(ax_spec_eth.get_xticks())
        ax_spec_eth.set_title(f'Significant difference in {type_name} DNA rate in these {col_name} - Ethnicity')
        labels = [tw.fill(l, 20) for l in DNA_eth_pvals_plt[col_name].unique()]
        ax_spec_eth.set_xticklabels(labels=labels, fontsize=8)
        plt.savefig(f'plots/AdHoc/AdHoc - {type_name} - Eth {col_name}.png', bbox_inches='tight')
        plt.close()
    else:
        print(f'No statistical difference in DNA rate for ethnicity by {col_name} for {type_name}')

slide_3('slc_desc', 'SLCs', IP_df, 'Inpatient')
slide_3('slc_desc', 'SLCs', OP_df, 'Outpatient')
slide_3('sl_desc', 'SLs', IP_df, 'Inpatient')
slide_3('sl_desc', 'SLs', OP_df, 'Outpatient')


# =============================================================================
# #IMD RTT LoW by SLC SLIDE 4
# =============================================================================

def slide_4(col, col_name, df, type_name):
    slc_unique = df[col].dropna().unique()
    IMD_1_2 = df.loc[df['IMD'] == 'IMD 1-2'].copy()
    IMD_3_10 = df.loc[df['IMD'] == 'IMD 3-10'].copy()
    #loop over each slc
    IMD_3_10_rates = []
    IMD_1_2_rates = []
    DNA_pvals = []
    for spec in slc_unique:
        IMD_1_2_spec = IMD_1_2.loc[IMD_1_2[col] == spec].copy()
        IMD_3_10_spec = IMD_3_10.loc[IMD_3_10[col] == spec].copy()
        #Only perform Mood's IMD_3_10dian test if there are t least 15 samples in each 
        #pop. Also, if both IMD_3_10dians are zero, IMD_3_10dian test cannot work properly, so 
        #exclude
        IMD_3_10_total, IMD_3_10_rate = df_to_dna_rate(IMD_3_10_spec)
        IMD_1_2_total, IMD_1_2_rate = df_to_dna_rate(IMD_1_2_spec)
        #significance testing
        if (IMD_1_2_spec.shape[0] >= 15) and (IMD_3_10_spec.shape[0] >= 15):
            #Test for statistical difference in wait length for IMD and Ethnicity
            eth_dna_z_value, eth_dna_pvalue = propHypothesisTest(IMD_3_10_rate, IMD_1_2_rate, IMD_3_10_total, IMD_1_2_total)
            DNA_pvals.append(eth_dna_pvalue)
        else:
            DNA_pvals.append(np.nan)
        #add rates
        IMD_3_10_rates.append(IMD_3_10_rate*100)
        IMD_1_2_rates.append(IMD_1_2_rate*100)

    #Make a df of the results
    DNA_IMD_pvals = pd.DataFrame({col_name:slc_unique, 
                                'p-value':DNA_pvals,
                                'DNA Rate IMD_1_2':IMD_1_2_rates,
                                'DNA Rate IMD_3_10':IMD_3_10_rates})
    #statistically significant results
    DNA_IMD_pvals = DNA_IMD_pvals.loc[DNA_IMD_pvals['p-value'] < 0.025].copy()

    DNA_IMD_pvals_plt = pd.melt(DNA_IMD_pvals.loc[DNA_IMD_pvals['p-value'] < 0.025].copy(),
                              id_vars=[col_name],
                              value_vars = ['DNA Rate IMD_1_2', 'DNA Rate IMD_3_10'])

    if len(DNA_IMD_pvals_plt) > 0:
        #Plot
        fig, ax_spec_eth = plt.subplots(1, 1, figsize = (24,6))
        sns.barplot(data=DNA_IMD_pvals_plt, x=col_name, hue = 'variable', y='value',
                    ax=ax_spec_eth, palette=['seagreen','lightgreen'])
        legend = ax_spec_eth.get_legend()
        # #Get seaborn legend
        handles = legend.legend_handles
        ax_spec_eth.legend(handles, ['IMD 1-2','IMD 3-10'])#, bbox_to_anchor = (1,1))
        ax_spec_eth.set_ylabel('DNA Rate')
        ax_spec_eth.set_xticks(ax_spec_eth.get_xticks())
        ax_spec_eth.set_title(f'Significant difference in {type_name} DNA rate in these {col_name} - IMD')
        labels = [tw.fill(l, 13) for l in DNA_IMD_pvals_plt[col_name].unique()]
        ax_spec_eth.set_xticklabels(labels=labels, fontsize=8)
        plt.savefig(f'plots/AdHoc/AdHoc - {type_name} - IMD {col_name}.png', bbox_inches='tight')
        plt.close()
    else:
        print(f'No statistical difference in DNA rate for IMD by {col_name} for {type_name}')


slide_4('slc_desc', 'SLCs', IP_df, 'Inpatient')
slide_4('slc_desc', 'SLCs', OP_df, 'Outpatient')
slide_4('sl_desc', 'SLs', IP_df, 'Inpatient')
slide_4('sl_desc', 'SLs', OP_df, 'Outpatient')



# MATERNITY PLOT
# ========================================================================
#     #>37 week gestation analysis SLIDE 5
# ========================================================================
####REDEFINE ABOVE FUNCTION BC I PULLED IT APART PREVIOUSLY.
#Function to plot labels on bars
def show_values_on_bars(axs,percentage = True, rounded = 2, numbers = None):
    def _show_on_single_plot(ax):
        counter = 0       
        for p in ax.patches:
            if p._height !=0:
                if percentage:
                    _x = p.get_x() + p.get_width() / 2
                    if (p.xy[1] == 0) and (p._height < 1):#p._height < 0.50:
                        _y = p.get_y() + p.get_height() + 0.01

                        if rounded == 2:
                            value = '{:.2f}'.format(p.get_height()*100)
                        elif rounded == 0:
                            value = '{:.0f}'.format(p.get_height()*100)
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


mat_df['IMD_decile'] = mat_df['IMD_decile'].astype(float)
#############All week waits
#####IMD
#Get a version with no NaNs for deciles
mat_df_IMD = mat_df.loc[~pd.isnull(mat_df['IMD_decile'])].copy()
#Make new columns with IMD1-2 or IMD3-10
mat_df_IMD['value'] = np.where(mat_df_IMD['IMD_decile'].isin([1, 2]), 'IMD 1-2', 'IMD 3-10')
mat_df_IMD['type'] = 'IMD'
#####Ethnicity
#Get a version without unknown ethnicities
mat_df_eth = mat_df.loc[~mat_df['ethnicity'].isin(['Unknown','Unwilling to answer'])].copy()
#Add column for white british and ethnic minority split
mat_df_eth['value'] = np.where(mat_df_eth['ethnicity'] == 'White British', 'White British', 'Ethnic Minority')
mat_df_eth['type'] = 'Ethnicity'

############# > 37 week gestation
#####IMD
#Filter to >37 weeks and remove decile nans
mat_df_IMD_37 = mat_df.loc[(mat_df['Gestation <37 weeks'] == 'Yes')
                          & (~pd.isnull(mat_df['IMD_decile']))].copy()
#Make columns for IMD and ethnicity as above
mat_df_IMD_37['value'] = np.where(mat_df_IMD_37['IMD_decile'].isin([1, 2]), 'IMD 1-2', 'IMD 3-10')
mat_df_IMD_37['type'] = 'IMD\n (<37 Week Gestation)'
#####Ethnicity
#Remove unknown ethnicities
mat_df_eth_37 = mat_df.loc[(mat_df['Gestation <37 weeks'] == 'Yes')
                          & (~mat_df['ethnicity'].isin(['Unknown', 'Unwilling to answer']))].copy()
#Add column for white british and ethnic minority split
mat_df_eth_37['value'] = np.where(mat_df_eth_37['ethnicity'] == 'White British', 'White British', 'Ethnic Minority')
mat_df_eth_37['type'] = 'Ethnicity \n(<37 Week Gestation)'

##############Hypothesis testing
#####IMD
#Get counts
total_num_IMD = mat_df_IMD.shape[0]
total_num_IMD_37 = mat_df_IMD_37.shape[0]
n_IMD12 = mat_df_IMD[mat_df_IMD['value'] == 'IMD 1-2'].shape[0]
n_IMD12_37 = mat_df_IMD_37[mat_df_IMD_37['value'] == 'IMD 1-2'].shape[0]
#test hypothesis
z, pval_mat_imd = propHypothesisTest((n_IMD12 / total_num_IMD),
                                     (n_IMD12_37 / total_num_IMD_37),
                                     total_num_IMD, total_num_IMD_37)

imd_str = 'a' if pval_mat_imd < 0.05 else 'no'

#####Ethnicity
#get counts
total_num_eth = mat_df_eth.shape[0]
total_num_eth_37 = mat_df_eth_37.shape[0]
n_em = mat_df_eth[mat_df_eth['value'] == 'Ethnic Minority'].shape[0]
n_em_37 = mat_df_eth_37[mat_df_eth_37['value'] == 'Ethnic Minority'].shape[0]
#test hypothesis
z, pval_mat_eth = propHypothesisTest((n_em / total_num_eth),
                                     (n_em_37 / total_num_eth_37),
                                     total_num_eth,  total_num_eth_37)
eth_str = 'a' if pval_mat_eth < 0.05 else 'no'

#####Plot
#Concat all dataframes into one large one
mat_in_full = pd.concat([mat_df_IMD, mat_df_eth, mat_df_IMD_37, mat_df_eth_37], ignore_index=True)  
#Plot a filled bar
fig, ax_52WW = plt.subplots(1,1)
hue_order = ['White British', 'Ethnic Minority', 'IMD 3-10', 'IMD 1-2']
sns.histplot(data=mat_in_full.sort_values(['type']), x='type', hue='value',
             multiple='fill', shrink=0.6, hue_order=hue_order,
             palette=['lightskyblue','royalblue','lightgreen','seagreen'],
             alpha=1, ax=ax_52WW)
ax_52WW.yaxis.set_major_formatter(PercentFormatter(1))
ax_52WW.set_xlabel('')
ax_52WW.set_ylabel('Percentage of Patients')
ax_52WW.set_title('Babies born in the last 12 months')
legend = ax_52WW.get_legend()
# #Get seaborn legend
handles = legend.legend_handles
ax_52WW.legend(handles,
               ['White British', 'Ethnic Minority', 'IMD 3-10', 'IMD 1-2'],
               bbox_to_anchor=(1,1))
#Make a list of the numbers to include under the percentages
numbers = [n_IMD12, n_IMD12_37, n_em, n_em_37]
totals = [total_num_IMD, total_num_IMD_37, total_num_eth, total_num_eth_37]
show_values_on_bars(ax_52WW, numbers = [i for i in numbers if i!=0])
show_totals(ax_52WW, totals)
#add text boxes
fig.text(.95, .4,
         tw.fill(f"There is {eth_str} significant difference "\
                 "between the proportion of ethnic minority "\
                 "babies born at <37 weeks gestation.", 35),
         ha='center', clip_on=False, fontsize=10,
         bbox=dict(boxstyle='round,pad=0.5', fc='none', ec='black'))
fig.text(.95, .2,
         tw.fill(f"There is {imd_str} significant difference  "\
                 "between the proportion of IMD 1&2 "\
                 "babies born at <37 weeks gestation.", 35),
         ha='center', clip_on=False, fontsize=10,
         bbox=dict(boxstyle='round,pad=0.5', fc='none', ec='black'))
plt.tight_layout()
plt.savefig('plots/AdHoc - maternity.png', bbox_inches='tight')
plt.close()
