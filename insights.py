"""
DS4300 - Final Project
DS Connect!
Generating Insights:
    After matching up our respondents with their "perfect" match, we want something to give them something more than
    just a name and email. So, this script generates some insights about why the pair was matched up and how
    they ranked compared to the rest of the class.
"""

import pandas as pd
import os
from compatibility import retrieve_data_mongo
from scipy.stats import percentileofscore


# %% Generate insights from responses (MongoDB) and matches (CSV)

def retrieve_data(*args):
    """ Connect to MongoDB and retrieve responses - Read in optimized matches as well """
    responses = retrieve_data_mongo(*args)
    matches_df = pd.read_csv("ds_connect_matches.csv")

    return responses, matches_df


def get_insights(responses, matches):
    """ Generate insights from our matches and each of the partner's respective responses """
    insights_df = matches.copy()
    responses_df = pd.DataFrame(responses)

    # Add new column which compares match's score to the class average
    average_score = insights_df['compatibility'].mean()
    insights_df['score_comparison'] = insights_df['compatibility'] - average_score

    # Calculate percentile of the pair's compatibility compared to the rest of the class
    insights_df['percentile'] = insights_df['score_comparison'] \
        .apply(lambda x: percentileofscore(insights_df['score_comparison'], x))

    # Identify the topics/questions that the respondents 100% agreed upon
    scopes = []
    for i, row in matches.iterrows():
        person1_responses = responses_df[responses_df['Name'] == row['person1_name']].iloc[0, 3:]
        person2_responses = responses_df[responses_df['Name'] == row['person2_name']].iloc[0, 3:]
        equal_cols = list(set(responses_df.columns[3:][person1_responses == person2_responses]))
        scopes.append(equal_cols)

    insights_df['equal_questions'] = scopes

    return insights_df


def clean_insights(insights):
    """ Clean up the dataframe: renaming columns, ordering, rounding """

    insights.columns = ['Person 1', 'Person 1 Email', 'Person 2 Name', 'Person 2 Email',
                        'Compatibility Score', 'Average Score Comparison', 'Class Percentile',
                        'Scopes of Compatibility']

    insights_df = insights.sort_values(by=['Compatibility Score'], ascending=False, ignore_index=True)

    insights_df['Compatibility Score'] = insights_df['Compatibility Score'].astype(float)
    insights_df['Average Score Comparison'] = insights_df['Average Score Comparison'].astype(float)
    insights_df['Class Percentile'] = insights_df['Class Percentile'].astype(float)
    insights_df.round({'Compatibility Score': 3, 'Average Score Comparison': 3, 'Class Percentile': 3})

    return insights_df


def main():
    # Connect to MongoDB and retrieve responses
    connection = "mongodb+srv://{}:{}@responses.97go3aq.mongodb.net/test"
    responses, matches = retrieve_data(os.environ['MONGO_USER'], os.environ['MONGO_PASSWORD'], connection,
                                       'compatibility', 'responses')

    # Generate insights and return as a CSV for easy readability!
    insights_df = get_insights(responses, matches)
    insights_df = clean_insights(insights_df)
    insights_df.to_csv('insights.csv')


if __name__ == "__main__":
    main()
