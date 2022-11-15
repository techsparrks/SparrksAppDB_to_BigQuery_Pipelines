import numpy as np
import pandas as pd


def calculate_booked(journey_analysis_df, topic_or_coach):
    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    journey_analysis_df['booked'] = journey_analysis_df.groupby(column_name)[column_name].transform(
        'count')
    journey_analysis_df['journey_completed'] = journey_analysis_df[journey_analysis_df['completed'] == 'x'].groupby(column_name)[
            column_name].transform('count')
    journey_analysis_df = journey_analysis_df[journey_analysis_df[column_name] != '']

    return journey_analysis_df


def calculate_booked_vs_completed(journey_analysis_df, topic_or_coach):
    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    journey_analysis_df['booked'] = journey_analysis_df.groupby(column_name)[column_name].transform(
        'count')
    journey_analysis_df['journey_completed'] = journey_analysis_df[journey_analysis_df['completed'] == 'x'].groupby(column_name)[
            column_name].transform('count')
    journey_analysis_df = journey_analysis_df[journey_analysis_df[column_name] != '']

    help_df = journey_analysis_df[[column_name, 'booked', 'journey_completed']].drop_duplicates()

    return help_df


def calculate_nps_sparrks_coaching(journey_analysis_df, topic_or_coach, nps_to_calculate, help_df):
    journey_analysis_df[nps_to_calculate].replace('', '0', inplace=True)
    journey_analysis_df[nps_to_calculate] = journey_analysis_df[nps_to_calculate].astype('int64')

    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    if nps_to_calculate == 'nps_power_coaching':
        column_output_name = 'nps_sparrks_coaching'
    elif nps_to_calculate == 'nps_coach':
        column_output_name = 'nps_coach'


    help_df['8'] = \
        journey_analysis_df[journey_analysis_df[nps_to_calculate] > 8].groupby(column_name)[column_name].transform(
            'count')
    help_df_1 = help_df[help_df['8'].notna()]

    help_df_2 = journey_analysis_df[[column_name]]
    help_df_2['7'] = journey_analysis_df[(journey_analysis_df[nps_to_calculate] < 7) & (
            journey_analysis_df[nps_to_calculate] != 0)].groupby(column_name)[column_name].transform('count')
    help_df_2 = help_df_2[help_df_2['7'].notna()].drop_duplicates()

    help_df_3 = journey_analysis_df[[column_name]]
    help_df_3['0'] = \
        journey_analysis_df[journey_analysis_df[nps_to_calculate] > 0].groupby(column_name)[column_name].transform(
            'count')
    help_df_3 = help_df_3[help_df_3['0'].notna()].drop_duplicates()

    merge_7_8 = pd.merge(help_df_1, help_df_2, on=column_name, how='outer')
    merge_7_8['8-7'] = merge_7_8['8'] - merge_7_8['7']

    final_df = pd.merge(merge_7_8, help_df_3, on=column_name, how='outer')
    final_df[column_output_name] = final_df['8-7'] / final_df['0']

    final_df = final_df[final_df['journey_completed'].notna()]

    return final_df[[column_name, column_output_name,'booked', 'journey_completed']]


def calc_feedback(journey_analysis_df, topic_or_coach):

    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    help_df = journey_analysis_df[[column_name]]
    help_df['feedback_n'] = \
        journey_analysis_df[journey_analysis_df['nps_power_coaching'] > 0].groupby(column_name)[column_name].transform(
            'count')
    help_df = help_df[help_df['feedback_n'].notna()].drop_duplicates()

    return help_df


def calc_nps_and_feedback(raw_journey_analysis_df, topic_or_coach):
    help_df = calculate_booked_vs_completed(raw_journey_analysis_df, topic_or_coach)
    nps_power_coaching_ratings_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, topic_or_coach,
                                                                   'nps_power_coaching', help_df)
    nps_coach_ratings_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, topic_or_coach, 'nps_coach', help_df)
    feedback_n_df = calc_feedback(raw_journey_analysis_df, topic_or_coach)
    ratings_df = pd.merge(nps_power_coaching_ratings_df, nps_coach_ratings_df, how='outer')
    final_ratings_df = pd.merge(ratings_df, feedback_n_df, how='outer')

    return final_ratings_df


def get_journey_analysis_data(gs_client, sales_funnel_doc_name, journey_analysis_sheet_name, nps_columns,
                              reference_names):
    # get journey analysis data
    doc = gs_client.open(sales_funnel_doc_name)
    journey_analysis_sheet_read = doc.worksheet(journey_analysis_sheet_name)
    journey_analysis_df = pd.DataFrame(journey_analysis_sheet_read.get_values())
    journey_analysis_df = journey_analysis_df.iloc[1:, :]
    journey_analysis_df.columns = journey_analysis_df.iloc[0]
    journey_analysis_df = journey_analysis_df[1:]
    journey_analysis_df.replace('', np.nan, inplace=True)
    raw_journey_analysis_df = journey_analysis_df[nps_columns].dropna(subset=[nps_columns[0]])
    # raw_journey_analysis_df = journey_analysis_df[journey_analysis_df[nps_columns].notna()]
    raw_journey_analysis_df = raw_journey_analysis_df.rename(columns=reference_names)

    return raw_journey_analysis_df
