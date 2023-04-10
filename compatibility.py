"""
Daniel Xu
DS4300 - Final Project
Compatibility Scoring Algorithm:
    Takes responses to DS Connect survey form and computes a similarity score between each possible match combination.
    Similarity scores and basic identity info will then be uploaded into Neo4J for further analysis.
"""

import pandas as pd
import os
from questions import QUESTIONS
from pymongo import MongoClient
from scipy.spatial import distance
from functools import reduce

RESPONSE_RELATIONSHIPS = [0] + ([1] * 17) + [0, 1, 1, 0, 1]  # 1 = Direct Relationship; 0 = Inverse Relationship


# %% Helper Funcs: Generate similarity scores for a sample of songs from given CSV

def invert_score(score, score_range):
    """ Alter scores for questions with responses that are more compatible if they're further apart... """
    return score_range[::-1][score - 1]


def create_tuning(responses1, responses2, weights=None):
    """ Identify most valued questions and use them to compute a list of uniquely weighted questions """
    if not weights:
        weights = [1]*23

    # Extract responders' most valued questions to compute a unique list of tuning weights
    first_factor1, second_factor1, \
        first_factor2, second_factor2 = (responses1['First Factor'], responses1['Second Factor'],
                                         responses2['First Factor'], responses2['Second Factor'])

    questions = list(QUESTIONS.keys())

    weights[questions.index(first_factor1)] *= 2
    weights[questions.index(first_factor2)] *= 2
    weights[questions.index(second_factor1)] *= 1.5
    weights[questions.index(second_factor2)] *= 1.5

    return weights


def score_compatibility(person1, person2, tuning_weights=None, response_relationships=None):
    """
    Vectorize song attributes given desired tuning params and compute cosine similarity
    :param person1: First person's responses as a dict
    :param person2: Second person's responses as a dict
    :param response_relationships: List of compatibility relationships for each question (direct/indirect relation)
    :param tuning_weights: Each response's weight to be factored into scoring
    :return: Dictionary of the two people's identifying attributes and their similarity score
    """
    if not response_relationships:
        response_relationships = [1] * 23
    responses1 = [v for v in list(person1.values())[4:27]]
    responses2 = [v for v in list(person2.values())[4:27]]

    # Modify second set of responses to match compatibility relationship for each question
    responses2 = [invert_score(score, range(1, 7)) if response_relationships[i] == 0 else score
                  for i, score in enumerate(responses2)]

    # Compute cosine similarity using song attribute vectors & weights
    cosine_sim = 1 - distance.cosine(responses1, responses2, w=tuning_weights)

    return {'person1_name': person1['Name'], 'person1_email': person1['Email'],
            'person2_name': person2['Name'], 'person2_email': person2['Email'],
            'similarity': cosine_sim}


def score_compatibilities(responses, **kwargs):
    """ Calculate similarity score for each unique pair of songs in song data """
    scores = []
    for i in range(len(responses)):
        for j in range(i + 1, len(responses)):
            # Take most valued compatibility questions to calculate tuning weights
            tuning_weights = create_tuning(responses[i], responses[j], kwargs.get('initial_weights', None))
            scores.append(score_compatibility(responses[i], responses[j], tuning_weights,
                                              kwargs.get('response_relationships', None)))

    return pd.DataFrame(scores)


def closest_scores(compatibility_scores, n):
    """ Return only the n most similar songs """
    return compatibility_scores.sort_values(by='similarity', ascending=False)[:n]


def compatibility_to_csv(matches, csv_name, path=None):
    """ Convert list of matches to csv readable for Neo4j """
    if path:
        output_file = os.path.join(path, csv_name)
        matches.to_csv(output_file, index=False)
    matches.to_csv(csv_name, index=False)


# %% Evo Algorithm - Maximize Total Compatibility of Classmates

def evaluate_scores(compatibilities):
    """ Evaluate total compatibility score of current iteration """
    return reduce(lambda x, y: x + y['Compatibility'], compatibilities, 0)


def switch_partners(all_compatibility_scores):
    """ Use list of all scores to optimize scores by switching partners """
    pass


def match_making(all_compatibility_scores):
    """ Evolutionary model to optimize total compatibility score across the class """
    pass


# %% Driver - Calculate compatibility scores and return matches as a CSV file

def retrieve_data_mongo(username, password, connection_str, db, collection):
    """ Connect to MongoDB Cluster and retrieve db/collection data """
    client = MongoClient(connection_str.format(username, password),
                         connectTimeoutMS=30000, socketTimeoutMS=None, connect=False, maxPoolsize=1)

    mongo_db = client[db]
    mongo_collection = mongo_db[collection]
    return [response for response in mongo_collection.find()]


if __name__ == "__main__":
    connection = "mongodb+srv://{}:{}@responses.97go3aq.mongodb.net/test"
    class_responses = retrieve_data_mongo(os.environ['MONGO_USER'], os.environ['MONGO_PASSWORD'], connection,
                                          'compatibility', 'responses')
    compatibilities = score_compatibilities(class_responses, response_relationships=RESPONSE_RELATIONSHIPS)
    compatibility_to_csv(compatibilities, "compatibilities.csv")
