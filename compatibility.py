"""
Daniel Xu
DS4300 - Final Project
Compatibility Scoring Algorithm:
    Takes responses to DS Connect survey and computes similarity score between each possible match combination.
    Optimize pairings across the class to produce results with the highest total compatibility score.
    Generate insights for each match and share results with our responders.
"""

import copy
import os
import pandas as pd
import random as rnd
from functools import reduce
from pprint import pprint
from pymongo import MongoClient
from scipy.spatial import distance
from questions import QUESTIONS
from evolution import Evo

# How responses to questions correlate (i.e. if people answer similarly, is that optimal for compatibility?) :
RESPONSE_RELATIONSHIPS = [0] + ([1] * 17) + [0, 1, 1, 0, 1]  # 1 = Direct Relationship; 0 = Inverse Relationship


# %% MongoDB / Data Conversion

def retrieve_data_mongo(username, password, connection_str, db, collection):
    """ Connect to MongoDB Cluster and retrieve db/collection data """
    client = MongoClient(connection_str.format(username, password),
                         connectTimeoutMS=30000, socketTimeoutMS=None, connect=False, maxPoolsize=1)

    mongo_db = client[db]
    mongo_collection = mongo_db[collection]
    return [response for response in mongo_collection.find()]


def compatibility_to_csv(matches, csv_name, path=None):
    """ Convert list of matches to csv readable for Neo4j """
    if path:
        output_file = os.path.join(path, csv_name)
        matches.to_csv(output_file, index=False)
    matches.to_csv(csv_name, index=False)


# %% Helper Funcs: Generate compatibility scores between all our participants

def invert_score(score, score_range):
    """ Alter scores for questions with responses that are more compatible if they're further apart... """
    return score_range[::-1][score - 1]


def create_tuning(responses1, responses2, weights=None):
    """
    Identify most valued questions and use them to compute a list of uniquely weighted questions
    :param responses1: first respondent's responses as a list
    :param responses2: second respondent's responses as a list
    :param weights: initial weights to be given to each question
    :return: modified tuning weights to be used for compatibility score calculation
    """
    if not weights:
        weights = [1] * 23

    # Extract responders' most valued questions to compute a unique list of tuning weights
    first_factor1, second_factor1, \
        first_factor2, second_factor2 = (responses1['First Factor'], responses1['Second Factor'],
                                         responses2['First Factor'], responses2['Second Factor'])

    questions = list(QUESTIONS.keys())

    weights[questions.index(first_factor1)] *= 3.5
    weights[questions.index(first_factor2)] *= 3.5
    weights[questions.index(second_factor1)] *= 2
    weights[questions.index(second_factor2)] *= 2

    return weights


def score_compatibility(person1, person2, tuning_weights=None, response_relationships=None):
    """
    Vectorize responses given desired tuning weights, then compute cosine similarity
    :param person1: First person's responses as a dict
    :param person2: Second person's responses as a dict
    :param response_relationships: List of compatibility relationships for each question (direct/indirect relation)
    :param tuning_weights: Each responder's question weights to be factored into scoring
    :return: Dictionary of the two individuals' PII and their similarity score
    """
    if not response_relationships:
        response_relationships = [1] * 23
    responses1 = [v for v in list(person1.values())[4:27]]
    responses2 = [v for v in list(person2.values())[4:27]]

    # Modify second set of responses to match compatibility relationship for each question
    responses2 = [invert_score(score, range(1, 7)) if response_relationships[i] == 0 else score
                  for i, score in enumerate(responses2)]

    # Compute cosine similarity using response vectors & weights
    cosine_sim = 1 - distance.cosine(responses1, responses2, w=tuning_weights)

    return {'person1_name': person1['Name'], 'person1_email': person1['Email'],
            'person2_name': person2['Name'], 'person2_email': person2['Email'],
            'compatibility': cosine_sim}


def score_compatibilities(responses, **kwargs):
    """ Calculate compatibility score for each unique pair of individuals """
    scores = []
    for i in range(len(responses)):
        for j in range(i + 1, len(responses)):
            # Take most valued compatibility questions to calculate tuning weights
            tuning_weights = create_tuning(responses[i], responses[j], kwargs.get('initial_weights'))
            scores.append(score_compatibility(responses[i], responses[j], tuning_weights,
                                              kwargs.get('response_relationships')))

    return scores


def closest_scores(compatibility_scores, n):
    """ Return the n most similar pairings """
    return compatibility_scores.sort_values(by='compatibility', ascending=False)[:n]


# %% Evo Algorithm - Maximize Total Compatibility of Classmates

def random_pairing(all_compatibility_scores):
    """ Generate First Solution: Randomly pair participants up """
    # Obtain list of all unique people
    all_people = set([pairing['person2_name'] for pairing in all_compatibility_scores]
                     + [all_compatibility_scores[0]['person1_name']])
    selected_people = set()
    matches = []

    rnd.shuffle(all_compatibility_scores)

    for pairing in all_compatibility_scores:
        people = [pairing['person1_name'], pairing['person2_name']]

        # Check if any person in the pairing has already been selected
        if any(person in selected_people for person in people):
            continue

        # Add the pairing to the matches list and update the selected_people list
        selected_people.update(people)
        matches.append(pairing)

        # Check if everyone has been paired up
        if selected_people == all_people:
            break

    return matches


def find_pairing(name1, name2, all_scores):
    """ Given two names, find their pairing in list of all possible matches """
    for match in all_scores:
        if match['person1_name'] == name1 or match['person2_name'] == name1:
            if match['person1_name'] == name2 or match['person2_name'] == name2:
                return match


def find_worst_pairing(pairings):
    """ Find the pairing with the lowest compatibility score lol """
    return min(pairings, key=lambda d: d.get("compatibility"))


def switch_random(pairings, all_scores):
    """ Change agent: switch pairings at random """
    # Create deepcopy for our  pairings
    scores = copy.deepcopy(pairings[0])

    # Take two random pairings and switch their partners
    first_pairing = rnd.choice(scores)
    scores.remove(first_pairing)
    second_pairing = rnd.choice(scores)
    scores.remove(second_pairing)

    scores.append(find_pairing(first_pairing['person1_name'], second_pairing['person1_name'], all_scores))
    scores.append(find_pairing(first_pairing['person2_name'], second_pairing['person2_name'], all_scores))

    return scores


def switch_worst_partners(pairings, all_scores):
    """ Change agent: Switching partners between the least compatible pairs """
    scores = copy.deepcopy(pairings[0])

    # Take our worst two pairings, remove them, and switch their partners
    worst_pairing = find_worst_pairing(scores)
    scores.remove(worst_pairing)
    second_worst_pairing = find_worst_pairing(scores)
    scores.remove(second_worst_pairing)

    scores.append(find_pairing(worst_pairing['person1_name'], second_worst_pairing['person1_name'], all_scores))
    scores.append(find_pairing(worst_pairing['person2_name'], second_worst_pairing['person2_name'], all_scores))

    return scores


def evaluate_scores(pairing):
    """ Fitness Criteria: Evaluate total compatibility score of current iteration """
    return reduce(lambda x, y: x + y['compatibility'], pairing, 0)


# %% Driver - Calculate compatibility scores and convert matches into a CSV file

def main():
    # Connect to MongoDB and retrieve responses
    connection = "mongodb+srv://{}:{}@responses.97go3aq.mongodb.net/test"
    class_responses = retrieve_data_mongo(os.environ['MONGO_USER'], os.environ['MONGO_PASSWORD'], connection,
                                          'compatibility', 'responses')

    all_pairings = score_compatibilities(class_responses, response_relationships=RESPONSE_RELATIONSHIPS)

    # Create Evo object to maximize compatibility across our dataset
    optum = Evo(all_pairings)

    # Register fitness criteria and agents
    optum.add_fitness_criteria("total_compatibility", evaluate_scores)

    optum.add_agent("switch random", switch_random, 1)
    optum.add_agent('switch worst partners', switch_worst_partners, 1)

    # Generate random solution to start off with
    optum.add_solution(random_pairing(all_pairings))

    # Run evolution model
    optum.evolve(10000, 500, 10000)
    optimal_matches = optum.best_solution()
    print('\nOptimal matches identified... here are the results:')
    pprint(optimal_matches)

    compatibility_to_csv(pd.DataFrame(optimal_matches[1]), 'ds_connect_matches.csv')


if __name__ == "__main__":
    main()
