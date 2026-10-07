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
    df['Ethnicity'] = np.where(df['Ethnicity'].isin(['Unknown', 'Not Known', 'Unwilling to answer',
                                                     'White British', 'White - British']),
                                                     'White British', 'Ethnic Minority')
    return df

# =============================================================================
# =============================================================================
# Establish SQL Engine and Read in data
# ============================================================================= 
# =============================================================================

t0 = time.time()     
conn_str = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/uhpbi conn string.txt', 'r')
uhpbi_engine = create_engine(conn_str.read())
conn_str.close()

print('Reading in data...')
SLs = pd.read_sql_query('SELECT DISTINCT(sl_desc) FROM  InfoDB.dbo.vw_cset_specialties',
                        uhpbi_engine).dropna()['sl_desc'].tolist()

# ===============================
# RTT Clock Stops
rtt_low_query = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/EPIC queries/RTT Clock Stops.sql', 'r')
rtt_low = data_cleaning(pd.read_sql_query(rtt_low_query.read(), uhpbi_engine))
rtt_low_query.close()
print('RTT Clock Stops')

# ===============================
# RTT 52 week waiters
rtt_incomp_query = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/EPIC queries/RTT 52ww.sql', 'r')
rtt_incomp = pd.read_sql_query(rtt_incomp_query.read(), uhpbi_engine)
perc = (rtt_incomp['Ethnicity'].count() / rtt_incomp.shape[0]) * 100
print(f'As of {version_date}, {perc:.2f}% of patients on the RTT wait list have an ethnicity recorded')
rtt_incomp = data_cleaning(rtt_incomp)
rtt_incomp_query.close()
print('RTT 52ww')

# ===============================
# Inpatient DNA Rate - ADD ONCE WRITTEN
#IP_dna_query = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/EPIC queries/Inpatient DNAs.sql', 'r')
#IP_dna = data_cleaning(pd.read_sql_query(IP_dna_query.read(), uhpbi_engine))
#IP_dna_query.close()
#print('Outpatient DNA rate')

# ===============================
# Outpatient DNA Rate
OP_dna_query = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/EPIC queries/Outpatient DNAs.sql', 'r')
OP_dna = data_cleaning(pd.read_sql_query(OP_dna_query.read(), uhpbi_engine))
OP_dna_query.close()
print('Outpatient DNA rate')

# ===============================
# F2F Appointments
F2F_query = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/EPIC queries/F2F.sql', 'r')
F2F = data_cleaning(pd.read_sql_query(F2F_query.read(), uhpbi_engine))
F2F_query.close()
print('F2F Appointments')

# ===============================
# Babies < 37 week gestation
baby_37w_query = open('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Inequalities/Health Inequalities Reporting/EPIC queries/Baby 37w.sql', 'r')
baby_37w = data_cleaning(pd.read_sql_query(baby_37w_query.read(), uhpbi_engine))
baby_37w_query.close()
print('Babies <37 weeks')

# ===============================
# Dispose connection
t1 = time.time()
print(f'Data read successfully in {t1-t0}')
uhpbi_engine.dispose()
#sdmart_engine.dispose()
# =============================================================================

SLs = pd.Series(rtt_low['SL'].tolist() + rtt_incomp['SL'].tolist() + OP_dna['SL'].tolist() + F2F['SL'].tolist()).drop_duplicates().dropna().sort_values().tolist()
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
#IP_dnas_lst = []
#IP_dna_values, IP_dna_totals, IP_dna_pvals = prop_vals(IP_dna, 'DNA')
#IP_dnas_lst.append(create_plot('Inpatients DNA Rate', 'DNA rate',  np.nan, IP_dna_values, IP_dna_totals, IP_dna_pvals, True))

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
    #IP_dna_values, IP_dna_totals, IP_dna_pvals = prop_vals(IP_dna.loc[IP_dna['SL'] == sl].copy(), 'DNA')
    #IP_dnas_lst.append(create_plot('Inpatients DNA Rate', 'DNA rate',  sl, IP_dna_values, IP_dna_totals, IP_dna_pvals, True))
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

# =============================================================================
# OVERALL TABLES
#List of columns for the summary tables
df_cols = ['Data Set', 'Service Line', 'Ethnicity', 'IMD', 'Sex', 'Minority Ethnic Rate',
           'White British Rate', 'IMD 1&2 Rate', 'IMD 3-10 Rate', 'Female Rate', 'Male Rate',
           'Minority Ethnic Total', 'White British Total', 'IMD 1&2 Total', 'IMD 3-10 Total',
           'Female Total', 'Male Total']

#Final Dfs
RTT_LoW = pd.DataFrame(rtt_low_lst, columns=df_cols)
RTT_52w = pd.DataFrame(rtt_52ww_lst, columns=df_cols)
#IP_dnas = pd.DataFrame(IP_dnas_lst, columns=df_cols)
OP_dnas = pd.DataFrame(OP_dnas_lst, columns=df_cols)
F2F_apt = pd.DataFrame(F2F_lst, columns=df_cols)
baby_37 = pd.DataFrame([baby_lst], columns=df_cols)

# =============================================================================
# SUMMARY DF
combined_cols = ['Service Line', 'Ethnicity', 'IMD', 'Sex']
dfs = [RTT_LoW, RTT_52w, F2F_apt, OP_dnas]#, IP_dnas]
new_dfs = []
new_cols = []
for df in dfs:
    temp = df.copy()
    name = temp['Data Set'].iloc[0]
    temp = temp[combined_cols].copy()
    add_cols = [(name, col) for col in combined_cols[1:]]
    new_cols += add_cols
    temp.columns = [('', 'Service Line')] + add_cols
    temp.set_index(('', 'Service Line'))
    new_dfs.append(temp)
key = new_dfs[0].columns[0] 
combination_df = pd.concat(new_dfs, axis=1, join="outer").set_index(key)[new_cols]
combination_df.index = pd.Series([idx[1] for idx in combination_df.index]).fillna('OVERALL')
combination_df.sort_index()

# =============================================================================
# TABLES FOR PAPER
replace = {'higher':'Significantly higher', 'lower':'Significantly Lower'}
low_replace = {'higher':'Significantly Longer', 'lower':'Significantly Shorter'}
nd = 'No Difference'
report_title = f'UHP IPR Reporting: 12 Months to {version_date}'

#RTT
RTT_low_sum = (RTT_LoW.loc[0, combined_cols[1:]].copy().replace(low_replace)
               .fillna(nd).rename('Median Length of Wait (weeks)'))
RTT_52w_sum = (RTT_52w.loc[0, combined_cols[1:]].copy().replace(replace)
               .fillna(nd).rename('52+ Week Waits as a % of total waiting'))
RTT_sum = pd.DataFrame([RTT_low_sum, RTT_52w_sum])

#F2F
F2F_sum = pd.DataFrame(F2F_apt.loc[0, combined_cols[1:]].replace(replace)
           .fillna(nd).rename('Non F2F appointments')).T

#DNA rate
#IP_dnas_sum = (IP_dnas.loc[0, combined_cols[1:]].replace(replace)
#               .fillna(nd).rename('Inpatient DNAs'))
IP_dnas_sum = pd.Series({'Ethnicity' : '-', 'IMD':'-', 'Sex':'-'}, name = 'Inpatient DNAs')
OP_dnas_sum = (OP_dnas.loc[0, combined_cols[1:]].replace(replace)
               .fillna(nd).rename('Outpatient DNAs'))
DNA_sum = pd.DataFrame([IP_dnas_sum, OP_dnas_sum])

#Babies 37 weeks
baby_37_sum = pd.DataFrame(baby_37.loc[0, combined_cols[1:]].replace(replace)
               .fillna(nd).rename('Proportion of babies born under 27 weeks gestation')).T


# =============================================================================
# SUMMARY BULLET POINTS

#headlines for existing measures
RTT_low_sum.loc[RTT_low_sum.str.contains('Significantly')]
name = 'RTT waits'
def headline_str(df, name, headline):
    #List of patient types
    replace = {'IMD': 'IMD 1&2',
               'Sex':'Female',
               'Ethnicity':'ethnic minority'}
    #Data frame of all the constituent string parts
    sigs = df[df != 'No Difference'].dropna(axis=1).T.reset_index()
    sigs.columns = ['index', 'value']
    sigs['pat'] = sigs['index'].str.replace(replace, regex=True)
    sigs['type'] = sigs['value'].str.split(' ').str[-1].str.lower()
    sigs['name'] = name
    if headline:
        sigs['headline'] = sigs['index'].isin(['Ethnicity', 'IMD'])
    else:
        sigs['headline'] = False
    #Group up
    grouped_sigs = sigs.groupby(['name','type', 'headline'] , as_index=False)['pat'].agg(lambda x: ', '.join(x))
    return grouped_sigs

def join_and(x):
    x = list(x)
    return ', '.join(x[:-1]) + ' and ' + x[-1] if len(x) > 1 else x[0]

sigs = pd.concat([
                headline_str(pd.DataFrame(RTT_low_sum).T, 'RTT waits', True),
                headline_str(pd.DataFrame(RTT_52w_sum).T, 'proportion of 52+ week waits', True),
                headline_str(F2F_sum, 'proportion of non-face-to-face-appointments', True),
                #headline_str(pd.DataFrame(IP_dnas_sum), 'Inpatient DNA rate', False),
                headline_str(pd.DataFrame(OP_dnas_sum).T, 'Outpatient DNA rate', False),
                headline_str(baby_37_sum, 'proportion of babies born at <37 weeks', False)])

head = sigs.loc[sigs['headline']].groupby(['name', 'type'], as_index=False)['pat'].agg(lambda x: join_and(x))
head['string'] = 'Significantly ' + head['type'] + ' ' + head['name'] + ' for ' + head['pat'] + ' patients'
headline_string = ' \n '.join(head['string'])
print('HEADLINES:')
print(headline_string)

new = sigs.loc[~sigs['headline']].groupby(['name', 'type'], as_index=False)['pat'].agg(lambda x: join_and(x))
new['string'] = 'Significantly ' + new['type'] + ' ' + new['name'] + ' for ' + new['pat'] + ' patients'
new_string = ' \n '.join(new['string'])
print('NEW:')
print(new_string)

# =============================================================================

# =============================================================================
# =============================================================================
# EXCEL OUTPUT
# =============================================================================
# =============================================================================

# =============================================================================
#Create Excel output
with pd.ExcelWriter(f'Tables/HI Summary Table - {version_date}.xlsx', engine='xlsxwriter') as writer:
    workbook = writer.book
# =============================================================================
#Combination sheet
    worksheet = workbook.add_worksheet("Combination")
    combination_df.to_excel(writer, sheet_name='Combination', startrow=2, startcol=1)

    group_names = {"RTT Current LoW": "Median LoW", "RTT 52+ Week Waiters": "52+ Week Wait",
                   "Non-F2F Appointments": "Non F2F Appointments",
                   "Outpatients DNA Rate": "Outpatient DNAs",}

    #Title formats
    service_line =  workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "border":2})
    group_header = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "border":1, "right":2})
    subheader = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "border":1, "bottom":2})
    r_subheader = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "border":1, "bottom":2, "right":2})

    # Service-line heading.
    worksheet.merge_range(1, 1, 2, 1, "Service Line", group_header)
    # Grouped and merged headers.
    start_col = 2
    position = 0
    while position < len(combination_df.columns):
        group = combination_df.columns[position][0]
        group_columns = []
        #Get list of tuples for that metric
        while (position < len(combination_df.columns) and combination_df.columns[position][0] == group):
            group_columns.append(combination_df.columns[position])
            position += 1
        end_col = start_col + len(group_columns) - 1
        display_name = group_names.get(group, group)
        #Write and Merge header
        worksheet.merge_range(1, start_col, 1, end_col, display_name, group_header)
        #Write subheaders
        for offset, (_, category) in enumerate(group_columns):
            if category == 'Sex':
                worksheet.write(2, start_col + offset, category, r_subheader)
            else:
                worksheet.write(2, start_col + offset, category, subheader)
        start_col = end_col + 1

    #More formats
    service_line = workbook.add_format({"bold":True, "align": "left", "valign": "vcenter", "right":2})
    result_format = workbook.add_format({"align": "center", "valign": "vcenter", "right":1})
    r_border = workbook.add_format({"align": "center", "valign": "vcenter", "right":2})
    b_border = workbook.add_format({"bottom":2})
    t_border = workbook.add_format({"top":2})
    #apply formats
    worksheet.set_column(2, len(combination_df.columns), 12, result_format)
    for col in [1, 4, 7, 10, 13]:
        worksheet.set_column(col, col, 12, r_border) #results formatting
    worksheet.set_column(1, 1, 45, service_line)  # Service Line
    #merge around edge
    worksheet.merge_range(0, 0, 0, 14, "", b_border)
    end = len(combination_df) + 3
    worksheet.merge_range(1, 0, end-1, 0, "", r_border)
    worksheet.merge_range(end, 0, end, 14, "", t_border)
    worksheet.merge_range(1, 14, end-1, 14, "")


# =============================================================================
# Summary tables
    worksheet = workbook.add_worksheet("Summary tables")

    title_format = workbook.add_format({"font_color": "#FFFFFF", "bg_color": "#003087",
                                        "align": "left", "valign": "vcenter", "border":2})
    measure_header = workbook.add_format({"align": "left", "valign": "vcenter", "border":1})
    category_header = workbook.add_format({"align": "center", "valign": "vcenter", "border":1})
    measure_format = workbook.add_format({"align": "left", "valign": "vcenter", "border":1})
    significant_format = workbook.add_format({"font_color": "#FFFFFF", "bg_color": "#DA291C",
                                              "align": "center", "valign": "vcenter", "border":1})
    no_difference_format = workbook.add_format({"font_color": "#FFFFFF", "bg_color": "#009639",
                                                "align": "center", "valign": "vcenter", "border":1})
    neutral_format = workbook.add_format({"align": "center", "valign": "vcenter", "border":1})

    worksheet.set_column("A:A", 3)
    worksheet.set_column("B:B", 52)
    worksheet.set_column("C:E", 21)

    def result_format(value):
        text = str(value).strip().lower()
        if text == "no difference":
            return no_difference_format
        if text.startswith("significantly"):
            return significant_format
        return neutral_format

    def write_block(title_row, summary_df):
        """
        title_row uses XlsxWriter's zero-based row numbering.
        For example, title_row=1 writes to Excel row 2.
        """
        worksheet.merge_range(title_row, 1, title_row, 4, report_title, title_format)
        worksheet.write(title_row + 1, 1, "Measure", measure_header)
        worksheet.write(title_row + 1, 2, "Ethnicity", category_header)
        worksheet.write(title_row + 1, 3, "Deprivation", category_header)
        worksheet.write(title_row + 1, 4, "Sex", category_header)

        for offset, (measure, row) in enumerate( summary_df.iterrows(), start=2):
            excel_row = title_row + offset
            worksheet.write(excel_row, 1, measure, measure_format)

            for excel_col, source_col in zip(range(2, 5), ["Ethnicity", "IMD", "Sex"]):
                value = row[source_col]

                if pd.isna(value):
                    value = ""

                worksheet.write(excel_row, excel_col, value, result_format(value))

    # Positions reproduce Summary tables!B2:E18.
    write_block(1, RTT_sum)
    write_block(6, F2F_sum)
    write_block(10, DNA_sum)
    write_block(15, baby_37_sum)

    wrap = workbook.add_format({'text_wrap': True})
    worksheet.merge_range(3, 6, 10, 10, headline_string, wrap)
    worksheet.merge_range(13, 6, 20, 10, new_string, wrap)

# =============================================================================
#other sheets
    RTT_LoW.to_excel(writer, sheet_name='Median LoW', index=False)
    RTT_52w.to_excel(writer, sheet_name='52+ Week Wait', index=False)
    #IP_dnas.to_excel(writer, sheet_name='Inpatient DNAs', index=False)
    OP_dnas.to_excel(writer, sheet_name='Outpatient DNAs', index=False)
    F2F_apt.to_excel(writer, sheet_name='Non F2F Appts', index=False)
    baby_37.to_excel(writer, sheet_name='Babies <37 Weeks', index=False)

t2 = time.time()
print(f'complete in {t2-t0}')