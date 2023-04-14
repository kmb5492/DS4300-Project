"""
Daniel Xu
DS4300 - Final Project
Evo Framework for Compatibility Score Match-Making:
    This Evo framework was sampled from our DS3500 class and has a few minor tweaks to allow it to
    properly read and iterate on our compatibility scores data
"""

import random as rnd
import copy
from functools import reduce


class Evo:

    def __init__(self, all_scores):
        """ Population constructor """
        self.all_scores = all_scores
        self.pop = {}  # The solution population eval -> solution
        self.fitness = {}  # Registered fitness functions: name -> objective function
        self.agents = {}  # Registered agents:  name -> (operator, num_solutions_input)

    def size(self):
        """ The size of the current population """
        return len(self.pop)

    def add_fitness_criteria(self, name, f):
        """ Register a fitness criterion (objective) with the
        environment. Any solution added to the environment is scored
        according to this objective """
        self.fitness[name] = f

    def add_agent(self, name, op, k=1):
        """ Register a named agent with the population.
        The operator (op) function defines what the agent does.
        k defines the number of solutions the agent operates on. """
        self.agents[name] = (op, k)

    def add_solution(self, sol):
        """ Add a solution to the population """
        # eval = ((obj1, score1), (obj2, score2).....)
        eval = tuple((name, f(sol)) for name, f in self.fitness.items())
        self.pop[eval] = sol

    def run_agent(self, name):
        """ Invoke an agent against the population """
        op, k = self.agents[name]
        picks = self.get_random_solutions(k)
        new_solution = op(picks, self.all_scores)
        self.add_solution(new_solution)

    def evolve(self, n=1, dom=100, status=100):
        """ Run n random agents (default=1)
        dom defines how often we remove dominated (unfit) solutions
        status defines how often we display the current population """
        agent_names = list(self.agents.keys())

        for i in range(n):
            pick = rnd.choice(agent_names)
            self.run_agent(pick)

            if i % dom == 0:
                self.remove_dominated()

            if i % status == 0:
                self.remove_dominated()
                print("Iteration:", i)
                print("Population size:", self.size())
                print("Highest calculated compatibility so far: \n", self.best_solution()[0][0])

        # Clean up the population
        self.remove_dominated()

    def get_random_solutions(self, k=1):
        """ Pick k random solutions from the population """
        if self.size() == 0:
            return []
        else:
            solutions = tuple(self.pop.values())
            return [copy.deepcopy(rnd.choice(solutions)) for _ in range(k)]


    @staticmethod
    def _dominates(p, q, filter_lim=-.05):
        """ p = evaluation of solution: ((obj1, score1), (obj2, score2), ... )"""
        return q[0][1] - p[0][1] <= filter_lim

    @staticmethod
    def _reduce_nds(S, p):
        return S - {q for q in S if Evo._dominates(p, q)}

    def remove_dominated(self):
        """ Remove dominated solutions """
        nds = reduce(Evo._reduce_nds, self.pop.keys(), self.pop.keys())
        self.pop = {k: self.pop[k] for k in nds}

    def best_solution(self):
        """ Return best solution from population """
        solution = max(self.pop.items(), key=lambda x: x[0][0][1])
        print(f'Out of the {self.size()} feasible combinations of parings, here is the optimal solution: \n', solution)
        return solution

    def __str__(self):
        """ Output the ten first solutions in the population """
        result = ""
        for evaluation, sol in self.pop.items()[10]:
            result += str(dict(eval)) + ":\t" + str(sol) + "\n"
        return result
