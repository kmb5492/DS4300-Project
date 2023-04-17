import pandas as pd
import os
from compatibility import retrieve_data_mongo
from scipy.stats import percentileofscore


def read_files():
    # Connect to MongoDB and retrieve responses
    connection = "mongodb+srv://{}:{}@responses.97go3aq.mongodb.net/test"
    os.environ['MONGO_USER'] = 'shareduser'
    os.environ['MONGO_PASSWORD'] = 'dcpLNy6o53BJmEXz'
    responses = retrieve_data_mongo(os.environ['MONGO_USER'], os.environ['MONGO_PASSWORD'], connection,
                                    'compatibility', 'responses')

    # Read in optimized solutions csv
    matches_df = pd.read_csv("ds_connect_matches.csv")

    return responses, matches_df


def get_insights(responses, matches):
    insights_df = matches.copy()
    responses_df = pd.DataFrame(responses)

    # Add a new column to the DataFrame to show how each match's score compares to the average
    average_score = insights_df['compatibility'].mean()
    insights_df['score_comparison'] = insights_df['compatibility'] - average_score

    # Calculate the percentile of score_difference column
    insights_df['percentile'] = insights_df['score_comparison'].apply \
        (lambda x: percentileofscore(insights_df['score_comparison'], x))

    # Identify scopes of questions that had the same response from each match
    scopes = []
    for i, row in matches.iterrows():
        person1_name = row['person1_name']
        person2_name = row['person2_name']
        person1_responses = responses_df[responses_df['Name'] == person1_name].iloc[0, 3:]
        person2_responses = responses_df[responses_df['Name'] == person2_name].iloc[0, 3:]
        equal_cols = list(set(responses_df.columns[3:][person1_responses == person2_responses]))
        scopes.append(equal_cols)

    # Add scopes of compatibility to insights
    insights_df['equal_questions'] = scopes

    # Rename columns
    insights_df.columns = ['Person 1', 'Person 1 Email', 'Person 2 Name', 'Person 2 Email',
                           'Compatibility Score', 'Average Score Comparison', 'Class Percentile',
                           'Scopes of Compatibility']

    # Sort matches by compatibility score in descending order
    insights_df = insights_df.sort_values(by=['Compatibility Score'], ascending=False, ignore_index=True)

    # Convert columns to numeric type
    insights_df['Compatibility Score'] = insights_df['Compatibility Score'].astype(float)
    insights_df['Average Score Comparison'] = insights_df['Average Score Comparison'].astype(float)
    insights_df['Class Percentile'] = insights_df['Class Percentile'].astype(float)

    # Round columns to nearest 3rd decimal
    insights_df[['Compatibility Score', 'Average Score Comparison', 'Class Percentile']] = round(
        insights_df[['Compatibility Score', 'Average Score Comparison', 'Class Percentile']], 3)

    return insights_df


def main():
    responses, matches = read_files()
    insights_df = get_insights(responses, matches)

    insights_df.to_csv('insights.csv')


if __name__ == "__main__":
    main()
