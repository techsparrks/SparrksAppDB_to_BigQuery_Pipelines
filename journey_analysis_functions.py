import numpy as np
import pandas as pd


def calculate_booked_and_completed(raw_journey_analysis_df, topic_or_coach):
    """
    Calculates how many times a topic or coach has been booked and how many times the
    journey has been completed

    raw_journey_analysis_df: data frame with the raw journey analysis data
    topic_or_coach: specifies if the values should be calculated for topic or coach

    returns: a data frame with booked and completed numbers
    """
    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    raw_journey_analysis_df = raw_journey_analysis_df[raw_journey_analysis_df[column_name] != '']
    raw_journey_analysis_df['booked'] = raw_journey_analysis_df.groupby(column_name)[column_name].transform(
        'count')
    raw_journey_analysis_df['journey_completed'] = raw_journey_analysis_df[raw_journey_analysis_df['completed'] == 'x'].groupby(column_name)[
            column_name].transform('count')
    raw_journey_analysis_df = raw_journey_analysis_df[raw_journey_analysis_df['journey_completed'].notnull()]

    booked_and_completed_df = raw_journey_analysis_df[[column_name, 'booked', 'journey_completed']].drop_duplicates()

    return booked_and_completed_df


def calculate_nps_sparrks_coaching(raw_journey_analysis_df, topic_or_coach, nps_to_calculate):
    """
    Calculates the NPS values Sparrks Coaching and Coach

    raw_journey_analysis_df: data frame with the raw journey analysis data
    topic_or_coach: specifies if the values should be calculated for topic or coach
    nps_to_calculate: specifies if NPS Sparrks Coaching or NPS Coach should be calculated

    returns: data frame with the calculated NPS values for either topic or coach
    """

    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    if nps_to_calculate == 'nps_power_coaching':
        column_output_name = 'nps_sparrks_coaching'
    elif nps_to_calculate == 'nps_coach':
        column_output_name = 'nps_coach'

    # change data type of the nps to calculate to integer
    raw_journey_analysis_df[nps_to_calculate] = raw_journey_analysis_df[nps_to_calculate].astype('int64')

    # count the number of ratings higher than 8, lower than 7 and higher than 0
    raw_journey_analysis_df['8'] = \
        raw_journey_analysis_df[raw_journey_analysis_df[nps_to_calculate] > 8].groupby(column_name)[column_name].transform(
            'count')
    raw_journey_analysis_df['7'] = \
        raw_journey_analysis_df[(raw_journey_analysis_df[nps_to_calculate] < 7) & (raw_journey_analysis_df[nps_to_calculate] != 0)].groupby(column_name)[column_name].transform(
            'count')
    raw_journey_analysis_df['0'] = \
        raw_journey_analysis_df[raw_journey_analysis_df[nps_to_calculate] > 0].groupby(column_name)[column_name].transform(
            'count')
    help_df_1 = raw_journey_analysis_df[raw_journey_analysis_df['8'].notna()][[column_name, '8']].drop_duplicates()
    help_df_2 = raw_journey_analysis_df[raw_journey_analysis_df['7'].notna()][[column_name, '7']].drop_duplicates()
    help_df_3 = raw_journey_analysis_df[raw_journey_analysis_df['0'].notna()][[column_name, '0']].drop_duplicates()

    # merge the help tables to calculate the number of ratings between 8 and 7
    merge_7_8 = pd.merge(help_df_1, help_df_2, on=column_name, how='outer')
    merge_7_8['8'].replace(np.nan, 0, inplace=True)
    merge_7_8['7'].replace(np.nan, 0, inplace=True)
    merge_7_8['8-7'] = merge_7_8['8'] - merge_7_8['7']

    # merge the help tables to calculate the NPS by dividing the number of ratings between 8 and 7
    # by the number of all ratings
    final_df = pd.merge(merge_7_8, help_df_3, on=column_name, how='outer')
    final_df['8-7'].replace(np.nan, 0, inplace=True)
    final_df[column_output_name] = final_df['8-7'] / final_df['0']

    return final_df[[column_name, column_output_name]]


def calc_feedback(raw_journey_analysis_df, topic_or_coach):
    """
    Calculate the number of feedbacks received by topic or coach

    raw_journey_analysis_df: data frame with the raw journey analysis data
    topic_or_coach: specifies if the values should be calculated for topic or coach

    returns: data frame with the number of feedbacks received
    """

    if topic_or_coach == 'topic':
        column_name = 'use_case_engl'
    elif topic_or_coach == 'coach':
        column_name = 'coach_name'

    # calculate the number of feedbacks received by counting all nps_power_coaching ratings bigger than 0
    raw_journey_analysis_df['feedback_n'] = \
        raw_journey_analysis_df[raw_journey_analysis_df['nps_power_coaching'] > 0].groupby(column_name)[column_name].transform(
            'count')
    # drop the values with no nps_power_coaching ratings and the duplicated values
    help_df = raw_journey_analysis_df[raw_journey_analysis_df['feedback_n'].notna()][[column_name, 'feedback_n']].drop_duplicates()

    return help_df


def calc_nps_and_feedback(raw_journey_analysis_df, topic_or_coach):
    """
    Method to calculate NPS and feedback values based on the raw data from the journey analysis

    raw_journey_analysis_df: data frame with the raw journey analysis data
    topic_or_coach: specifies if the values should be calculated for topic or coach

    returns: data frame with all necessary calculations for NPS and feedback
    """
    # first calculate the columns separately: booked, journey_completed, nps_power_coaching, nps_coach, feedback_n
    booked_and_completed_df = calculate_booked_and_completed(raw_journey_analysis_df, topic_or_coach)
    nps_power_coaching_ratings_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, topic_or_coach, 'nps_power_coaching')
    nps_coach_ratings_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, topic_or_coach, 'nps_coach')
    feedback_n_df = calc_feedback(raw_journey_analysis_df, topic_or_coach)

    # merge tables to create a single view
    help_df_power_coaching = pd.merge(booked_and_completed_df, nps_power_coaching_ratings_df, how='outer')
    help_df_power_coaching['journey_completed'] = help_df_power_coaching['journey_completed'].replace(np.nan, 0)

    ratings_df = pd.merge(help_df_power_coaching, nps_coach_ratings_df, how='outer')
    ratings_df['nps_sparrks_coaching'].replace(np.nan, 0, inplace=True)
    ratings_df['nps_coach'].replace(np.nan, 0, inplace=True)

    feedback_ratings_df = pd.merge(ratings_df, feedback_n_df, how='outer')
    feedback_ratings_df['feedback_n'].replace(np.nan, 0, inplace=True)
    feedback_ratings_df['booked'].replace(np.nan, 0, inplace=True)

    # calculate feedback % by dividing the number of all feedbacks by the number of completed journeys
    feedback_ratings_df["feedback_p"] = round((feedback_ratings_df["feedback_n"] / feedback_ratings_df["journey_completed"]), 2)
    feedback_ratings_df['feedback_p'].replace(np.inf, 0, inplace=True)

    # set the origin column to OLD VERSION to differentiate from the NEW VERSION data
    feedback_ratings_df["origin"] = "OLD VERSION"

    print('Data for the', topic_or_coach, 'KPI is ready to be uploaded!')

    return feedback_ratings_df


def get_journey_analysis_data(gs_client, google_sheets_doc_name, google_sheets_sheet_name, nps_columns,
                              reference_names):
    """
    Reads the raw data from Google Sheet

    gs_client: Google Sheets client
    google_sheets_doc_name: the name of the Google Sheets document to read data from
    google_sheets_sheet_name: the name of the Google Sheets sheet to read data from
    nps_columns: list of relevant columns
    reference_names: dictionary used to rename the Google Sheet column names

    returns: a DataFrame with the raw data from Google Sheets
    """

    # get journey analysis data and put it into a DataFrame
    doc = gs_client.open(google_sheets_doc_name)
    journey_analysis_sheet_read = doc.worksheet(google_sheets_sheet_name)
    journey_analysis_df = pd.DataFrame(journey_analysis_sheet_read.get_values())

    # set first row as a header for the DataFrame
    journey_analysis_df.columns = journey_analysis_df.iloc[0]
    journey_analysis_df = journey_analysis_df[1:]

    # replace empty strings with 0
    journey_analysis_df.replace('', 0, inplace=True)

    # drop rows for which the use case is a NaN value
    raw_journey_analysis_df = journey_analysis_df[nps_columns].dropna(subset=['Use Case engl.'])

    # rename columns to match the BigQuery schema
    raw_journey_analysis_df = raw_journey_analysis_df.rename(columns=reference_names)

    return raw_journey_analysis_df