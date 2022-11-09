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


def calculate_nps_sparrks_coaching(journey_analysis_df, topic_or_coach, nps_to_calculate):
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

    journey_analysis_df['booked'] = journey_analysis_df.groupby(column_name)[column_name].transform(
        'count')
    journey_analysis_df['journey_completed'] = journey_analysis_df[journey_analysis_df['completed'] == 'x'].groupby(column_name)[
            column_name].transform('count')
    journey_analysis_df = journey_analysis_df[journey_analysis_df[column_name] != '']

    help_df_1 = journey_analysis_df[[column_name, 'booked', 'journey_completed']]
    help_df_1['8'] = \
        journey_analysis_df[journey_analysis_df[nps_to_calculate] > 8].groupby(column_name)[column_name].transform(
            'count')
    help_df_1 = help_df_1[help_df_1['8'].notna()].drop_duplicates()

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
